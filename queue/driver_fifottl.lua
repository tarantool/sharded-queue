local vshard = require('vshard')
local state = require('queue.state')
local utils = require('queue.utils')
local time = require('queue.time')
local fiber = require('fiber')

local driver = {}

local index = {
    task_id    = 1,
    bucket_id  = 2,
    status     = 3,
    created    = 4,
    priority   = 5,
    ttl        = 6,
    next_event = 7,
    data       = 8,
    index      = 9
}

-- FIBERS METHODs --

local function fiber_iteration(tube_name, processed)
    local curr = time.time()
    local task = nil
    local estimated = time.TIMEOUT_INFINITY

    -- delayed tasks

    task = nil  
end

local function fiber_common(tube_name)
    fiber.name(tube_name)
    local processed = 0

    while true do
        if not box.cfg.read_only then
            local ok, ret = pcall(fiber_iteration, tube_name, processed)

            if not ok and not err.code == box.error.READONLY then
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

end

function driver.get_task(args)
    -- getting task without change state --
    local task = box.space[args.tube_name].index.status:min { state.READY }
    return task
end

function get_index(tube_name, bucket_id)
    local task = box.space[tube_name].index.idx:max { bucket_id }
    if not task or task[index.bucket_id] ~= bucket_id then
        return 1
    else
        return task[index.index] + 1
    end
end

function driver.put(args)

    local delay = args.delay or 0
    local priority = args.priority or 0
    local status = state.READY
    local ttl = args.ttl or time.TIMEOUT_INFINITY
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
        next_event = time.event(ttl)
    end

    local task = box.space[args.tube_name]:insert {
        task_id,                -- task_id
        args.bucket_id,         -- bucket_id
        status,                 -- state
        time.time(),            -- created
        priority,               -- priority
        time.time(ttl),         -- ttl
        next_event,             -- next_event
        args.data,              -- data
        idx                     -- index
    }
    return task
end

function driver.take(args)
    for _, task in
        box.space[args.tube_name].index.status:pairs(nil, {state.READY} ) do
        if task ~= nil and task[index.status] == state.READY then
            return box.space[args.tube_name]:update(task[index.task_id], { {'=', index.status, state.TAKEN} })
        end
    end
end

function driver.touch(tube_name, ttr)
    -- error('fifo queue does not support touch')
end

function driver.delete(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end
    return task
end
-- TODO: this 
function driver.release(args)

    local task = box.space[args.tube_name]:update(args.task_id, { {'=', index.status, state.READY} })
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
