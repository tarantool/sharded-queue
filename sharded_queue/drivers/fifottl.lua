local fiber = require('fiber')
local state = require('sharded_queue.state')
local utils = require('sharded_queue.utils')
local statistics = require('sharded_queue.statistics')
local time  = require('sharded_queue.time')
local log = require('log') -- luacheck: ignore

local index = {
    task_id    = 1,
    bucket_id  = 2,
    status     = 3,
    created    = 4,
    priority   = 5,
    ttl        = 6,
    ttr        = 7,
    next_event = 8,
    data       = 9,
    index      = 10
}

local function update_stat(tube_name, name)
    statistics.update(tube_name, name, '+', 1)
end

local function is_expired(task)
    return (task[index.created] + task[index.ttl]) <= time.cur()
end

local wait_cond_map = {}

local function wc_signal(tube_name)
    if wait_cond_map[tube_name] ~= nil then
        wait_cond_map[tube_name]:signal()
    end
end

local function wc_wait(tube_name, time)
    if wait_cond_map[tube_name] ~= nil then
        wait_cond_map[tube_name]:wait(time)
    end
end

-- FIBERS METHODS --
local function fiber_iteration(tube_name, processed)
    local cur  = time.cur()
    local estimated = time.MAX_TIMEOUT

    -- delayed tasks

    local task = box.space[tube_name].index.watch:min { state.DELAYED }
    if task ~= nil and task[index.status] == state.DELAYED then
        if cur >= task[index.next_event] then
            box.space[tube_name]:update(task[index.task_id], {
                { '=', index.status,     state.READY },
                { '=', index.next_event, task[index.created] + task[index.ttl] }
            })
            estimated = 0
            processed = processed + 1
        else
            estimated = time.sec(tonumber(task[index.next_event] - cur))
        end
    end

    -- ttl tasks

    for _, s in pairs({ state.READY, state.BURIED }) do
        task = box.space[tube_name].index.watch:min { s }
        if task ~= nil and task[index.status] == s then
            if cur >= task[index.next_event] then
                box.space[tube_name]:delete(task[index.task_id])
                update_stat(tube_name, 'delete')
                update_stat(tube_name, 'done')
                estimated = 0
                processed = processed + 1
            else
                local e = time.sec(tonumber(task[index.next_event] - cur))
                estimated = e < estimated and e or estimated
            end
        end
    end

    -- ttr tasks

    task = box.space[tube_name].index.watch:min { state.TAKEN }
    if task and task[index.status] == state.TAKEN then
        if cur >= task[index.next_event] then
            box.space[tube_name]:update(task[index.task_id], {
                { '=', index.status,     state.READY },
                { '=', index.next_event, task[index.created] + task[index.ttl] }
            })
            estimated = 0
            processed = processed + 1
        else
            local e = time.sec(tonumber(task[index.next_event] - cur))
            estimated = e < estimated and e or estimated
        end
    end

    if estimated > 0 or processed > 1000 then
        estimated = estimated > 0 and estimated or 0
        wc_wait(tube_name, estimated)
    end

    return processed
end

local function fiber_common(tube_name)
    fiber.name(tube_name)
    wait_cond_map[tube_name] = fiber.cond()

    local processed = 0

    while true do
        if not box.cfg.read_only then
            local ok, ret = pcall(fiber_iteration, tube_name, processed)
            if not ok and not (ret.code == box.error.READONLY) then
                return 1
            elseif ok then
                processed = ret
            end
        else
            fiber.sleep(0.1)
        end
    end
end

-- QUEUE METHODs --

local function tube_create(args)
    local space_options = {}

    local if_not_exists = args.options.if_not_exists or true

    space_options.if_not_exists = if_not_exists
    space_options.temporary = args.options.temporary or false
    space_options.engine = args.options.engine or 'memtx'

    space_options.format = {
        { 'task_id',    'unsigned' },
        { 'bucket_id',  'unsigned' },
        { 'status',     'string'   },
        { 'created',    'unsigned' },
        { 'priority',   'unsigned' },
        { 'ttl',        'unsigned' },
        { 'ttr',        'unsigned' },
        { 'next_event', 'unsigned' },
        { 'data',       '*'        },
        { 'index',      'unsigned' }
    }

    local space = box.schema.space.create(args.name, space_options)
    space:create_index('task_id', {
        type = 'tree',
        parts = {
            index.task_id, 'unsigned'
        },
        if_not_exists = if_not_exists
    })

    space:create_index('idx', {
        type = 'tree',
        parts = {
            index.bucket_id, 'unsigned',
            index.index,     'unsigned'
        },
        if_not_exists = if_not_exists
    })

    space:create_index('status',  {
        type = 'tree',
        parts = {
            index.status,   'string',
            index.priority, 'unsigned',
            index.created,  'unsigned'
        },
        unique = false,
        if_not_exists = if_not_exists
    })

    space:create_index('watch', {
        type = 'tree',
        parts = {
            index.status,     'string',
            index.next_event, 'unsigned'
        },
        unique = false,
        if_not_exists = if_not_exists
    })

    space:create_index('bucket_id', {
        parts = {
            index.bucket_id, 'unsigned',
        },
        unique = false,
        if_not_exists = if_not_exists
    })

    -- run fiber for tracking event
    fiber.create(fiber_common, args.name)
end

local function tube_drop(tube_name)
    box.space[tube_name]:drop()
end

local function get_index(tube_name, bucket_id)
    local task = box.space[tube_name].index.idx:max { bucket_id }
    if not task or task[index.bucket_id] ~= bucket_id then
        return 1
    else
        return task[index.index] + 1
    end
end

local function normalize_task(task)
    if task == nil then return nil end
    return { task.task_id, task.status, task.data }
end

local method = {}

function method.put(args)
    local delay = args.delay or 0
    -- setup params --

    local ttl = args.ttl or args.options.ttl or time.MAX_TIMEOUT
    local ttr = args.ttr or args.options.ttr or ttl
    local priority = args.priority or args.options.priority or 0

    local task = box.atomic(function()
        local idx = get_index(args.tube_name, args.bucket_id)

        local next_event
        local task_id = utils.pack_task_id(
            args.bucket_id,
            args.bucket_count,
            idx)

        local status = state.READY
        if delay > 0 then
            status = state.DELAYED
            ttl = ttl + delay
            next_event = time.event(delay)
        else
            next_event = time.event(ttl)
        end

        return box.space[args.tube_name]:insert {
            task_id,                -- task_id
            args.bucket_id,         -- bucket_id
            status,                 -- state
            time.cur(),             -- created
            priority,               -- priority
            time.nano(ttl),         -- ttl
            time.nano(ttr),         -- ttr
            next_event,             -- next_event
            args.data,              -- data
            idx                     -- index
        }
    end)

    update_stat(args.tube_name, 'put')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.take(args)
    local task = nil
    for _, tuple in
        box.space[args.tube_name].index.status:pairs( {state.READY} ) do
        if not is_expired(tuple) then
            task = tuple
            break
        end
    end

    if task == nil then return end

    local next_event = time.cur() + task[index.ttr]
    local dead_event = task[index.created] + task[index.ttl]

    if next_event > dead_event then
        next_event = dead_event
    end

    task = box.space[args.tube_name]:update(task[index.task_id], {
        { '=', index.status,     state.TAKEN },
        { '=', index.next_event, next_event  }
    })

    update_stat(args.tube_name, 'take')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.delete(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
    end

    update_stat(args.tube_name, 'delete')
    update_stat(args.tube_name, 'done')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.touch(args)
    local op = '+'
    if args.delta == time.TIMEOUT_INFINITY then
        op = '='
    end
    local task = box.space[args.tube_name]:update(args.task_id,
        {
            { op, index.next_event, args.delta },
            { op, index.ttl,        args.delta },
            { op, index.ttr,        args.delta }
        })

    update_stat(args.tube_name, 'touch')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.ack(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
    end

    update_stat(args.tube_name, 'ack')
    update_stat(args.tube_name, 'done')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.peek(args)
    return normalize_task(box.space[args.tube_name]:get(args.task_id))
end

function method.release(args)
    local task = box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.READY} })

    update_stat(args.tube_name, 'release')
    wc_signal(args.tube_name)
    return normalize_task(task)
end

function method.bury(args)
    update_stat(args.tube_name, 'bury')
    wc_signal(args.tube_name)
    return normalize_task(box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.BURIED} }))
end

-- unbury several tasks
function method.kick(args)
    for i = 1, args.count do
        local task = box.space[args.tube_name].index.status:min { state.BURIED }
        if task == nil then
            return i - 1
        end

        if task[index.status] ~= state.BURIED then
            return i - 1
        end

        box.space[args.tube_name]:update(task[index.task_id], { {'=', index.status, state.READY} })
        update_stat(args.tube_name, 'kick')
    end
    return args.count
end

return {
    create = tube_create,
    drop   = tube_drop,
    method = method
 }
