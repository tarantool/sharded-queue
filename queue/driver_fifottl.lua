local vshard = require('vshard')
local fiber = require('fiber')
local state = require('queue.state')
local utils = require('queue.utils')
local time = require('queue.time')
local log = require('log')
local driver = {}

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
    done = 2,
    take = 3,
    kick = 4,
    bury = 5,
    put  = 6,
    delete = 7,
    touch  = 8,
    ask    = 9
}

local function update_stat(tube_name, name)
    box.space._stat:update(tube_name, { {'+', stat_pos[name], 1} })
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

function driver.check(name)
    if box.space[name] ~= nil then
        return true
    else
        return false
    end
end


function driver.create(args)
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
    if not box.space._stat:get(args.name) then
        box.space._stat:insert {args.name, 0, 0, 0, 0, 0, 0, 0, 0}
    end
    -- run fiber for tracking event
    local tube_fiber = fiber.create(fiber_common, args.name)
end


function get_index(tube_name, bucket_id)
    local task = box.space[tube_name].index.idx:max { bucket_id }
    if not task or task[index.bucket_id] ~= bucket_id then
        return 1
    else
        return task[index.index] + 1
    end
end

function driver.statistic(args)
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

    stat.tasks.done = stored_stat[stat_pos.done]
    -- collect calls count
    stat.calls.take = stored_stat[stat_pos.take]
    stat.calls.kick = stored_stat[stat_pos.kick]
    stat.calls.bury = stored_stat[stat_pos.bury]
    stat.calls.put  = stored_stat[stat_pos.put]
    stat.calls.delete = stored_stat[stat_pos.delete]
    stat.calls.touch  = stored_stat[stat_pos.touch]
    stat.calls.ask    = stored_stat[stat_pos.ask]
    --
    return stat
end

function driver.put(args)
    local delay = args.delay or 0
    local priority = args.priority or 0
    local status = state.READY
    
    local ttl = args.ttl or time.MAX_TIMEOUT
    local ttr = args.ttr or ttl
    
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

    update_stat(args.tube_name, 'put')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.take(args)
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

    update_stat(args.tube_name, 'take')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.delete(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end

    update_stat(args.tube_name, 'delete')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.touch(args)
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
    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.ask(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end

    update_stat(args.tube_name, 'ask')
    update_stat(args.tube_name, 'done')
    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.peek(args)
    return box.space[args.tube_name].space:get(args.task_id)
end

function driver.release(args)
    local task = box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.READY} })

    fibers_info[args.tube_name].cond:signal()
    return task
end

function driver.bury(args)
    local task = box.space[args.tube_name]:update(args.id, { {'=', index.status, state.BURIED} })
    return task
end

-- unbury several tasks
function driver.kick(tube_name, count)
    for i = 1, count do
        local task = box.space[tube_name].index.status:min {state.BURIED}
        if task == nil then
            return i - 1
        end
        if task[2] ~= state.BURIED then
            return i - 1
        end

        task = box.space[tube_name]:update(task[1], { {'=', 2, state.READY} })
    end
    return count
end

return driver
