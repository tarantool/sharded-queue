local vshard = require('vshard')
local fiber = require('fiber')
local state = require('shared_queue.state')
local utils = require('shared_queue.utils')
local time  = require('shared_queue.time')
local log = require('log')

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

local stat_pos = {
    status = {
        done = 2
    },
    call = {
        take = 3,
        kick = 4,
        bury = 5,
        put  = 6,
        delete = 7,
        touch  = 8,
        ask    = 9,
        release = 10
    },
    options = {
        ttl = 11,
        ttr = 12,
        priority = 13
    }
}

local function update_stat(tube_name, class, name)
    box.space._stat:update(tube_name, { {'+', stat_pos[class][name], 1} })
end

local function is_expired(task)
    return (task[index.created] + task[index.ttl]) <= time.cur()
end

local fibers_info = {}

-- FIBERS METHODS --
local function fiber_iteration(tube_name, processed)
    local cur  = time.cur()
    local task = nil
    local estimated = time.MAX_TIMEOUT

    -- delayed tasks

    task = box.space[tube_name].index.watch:min { state.DELAYED }
    if task ~= nil and task[index.status] == state.DELAYED then
        if cur >= task[index.next_event] then
            task = box.space[tube_name]:update(task[index.task_id], {
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
                task = box.space[tube_name]:delete(task[index.task_id]):transform(index.status, 1, state.DONE)
                -- box.sequence[tube_name]:next()
                update_stat(tube_name, 'status', 'done')
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
            task = box.space[tube_name]:update(task[index.task_id], {
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
        
        local cond = fiber.cond()
        
        fibers_info[tube_name] = {
            cond = cond
        }

        cond:wait(estimated)
    end

    return processed
end

local function fiber_common(tube_name)
    fiber.name(tube_name)

    local processed = 0

    while true do
        if not box.cfg.read_only then
            local ok, ret = pcall(fiber_iteration, tube_name, processed)

            if not ok and not ret.code == box.error.READONLY then
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
            index.priority, 'unsigned'
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
    -- create stat record about new tube
    local ttl = args.options.ttl or time.MAX_TIMEOUT
    local ttr = args.options.ttr or ttl
    local priority = args.options.priority or 0

    if not box.space._stat:get(args.name) then
        box.space._stat:insert {
            args.name, 0, 0, 0, 0, 0, 0, 0, 0, 0, ttl, ttr, priority
        }
    end
    -- run fiber for tracking event
    local tube_fiber = fiber.create(fiber_common, args.name)
end

local function tubes()
    local tubes = {}
    for _, tuple in box.space._stat:pairs() do
        table.insert(tubes, tuple[1])
    end
    return tubes
end

local function tube_drop(tube_name)
    box.space._stat:delete(tube_name)
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

local method = {}

function method.statistic(args)
    --
    if not box.space[args.tube_name] then
        return nil
    end
    --
    local stat = {
        tasks = {},
        calls = {}
    }
    -- collecting tasks count
    local total = 0
    for name, value in pairs(state) do
        local count = box.space[args.tube_name].index.status:count(value)
        stat.tasks[name:lower()] = count
        total = total + count
    end
    stat.tasks.total = total

    local stored_stat = box.space._stat:get(args.tube_name)

    stat.tasks.done = stored_stat[stat_pos.status.done]
    -- collect calls count
    for call_name, index in pairs(stat_pos.call) do
        stat.calls[call_name] = stored_stat[index]
    end

    return stat
end


function method.put(args)
    local status = state.READY
    local delay = args.delay or 0
    -- setup params --
    local stat = box.space._stat:get(args.tube_name)

    local ttl = args.ttl or stat[stat_pos.options.ttl]
    local ttr = args.ttr or stat[stat_pos.options.ttr]
    local priority = args.priority or stat[stat_pos.options.priority]

    local idx = get_index(args.tube_name, args.bucket_id)

    local next_event
    local task_id = utils.pack_task_id(
        args.bucket_id,
        args.bucket_count,
        idx)

    if delay > 0 then
        status = state.DELAYED
        ttl = ttl + delay
        next_event = time.event(delay)
    else
        status = state.READY
        next_event = time.event(ttl)
    end

    local task = box.space[args.tube_name]:insert {
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

    update_stat(args.tube_name, 'call', 'put')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function method.take(args)
    local task = nil
    for _, tuple in
        box.space[args.tube_name].index.status:pairs(nil, {state.READY} ) do
        if tuple ~= nil and tuple[index.status] == state.READY then
            --
            task = tuple
            break
        end
    end

    if task == nil or is_expired(task) then
        return
    end

    local next_event = time.cur() + task[index.ttr]
    local dead_event = task[index.created] + task[index.ttl]
    
    if next_event > dead_event then
        next_event = dead_event
    end

    task = box.space[args.tube_name]:update(task[index.task_id], {
        { '=', index.status,     state.TAKEN },
        { '=', index.next_event, next_event  }
    })

    update_stat(args.tube_name, 'call', 'take')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function method.delete(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end

    update_stat(args.tube_name, 'call', 'delete')
    fibers_info[args.tube_name].cond:signal()
    return task
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

    update_stat(args.tube_name, 'call', 'touch')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function method.ask(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end

    update_stat(args.tube_name, 'call', 'ask')
    update_stat(args.tube_name, 'status', 'done')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function method.peek(args)
    return box.space[args.tube_name].space:get(args.task_id)
end

function method.release(args)
    local task = box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.READY} })
    
    update_stat(args.tube_name, 'call', 'release')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function method.bury(args)
    update_stat(args.tube_name, 'call', 'bury')
    fibers_info[args.tube_name].cond:signal()
    return box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.BURIED} })
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

        task = box.space[args.tube_name]:update(task[index.task_id], { {'=', index.status, state.READY} })
        update_stat(args.tube_name, 'call', 'kick')
    end
    return count
end

return {
    create = tube_create,
    drop   = tube_drop,
    tubes  = tubes,
    method = method
 }
