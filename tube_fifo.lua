local state = require('state')
local fiber = require('fiber')
local vshard = require('vshard')
local tube = {}

local MAX_TIMEOUT      = 365 * 86400 * 100       -- MAX_TIMEOUT == 100 years
local TIMEOUT_INFINITY = 18446744073709551615ULL -- Set to TIMEOUT_INFINITY

local function time(tm)
    if tm == nil then
        tm = fiber.time64()
    elseif tm < 0 then
        tm = 0
    else
        tm = tm * 1000000
    end
    return 0ULL + tm
end

local function event_time(tm)
    if tm == nil or tm < 0 then
        tm = 0
    end
    return 0ULL + tm * 1000000 + fiber.time64()
end

local index = {
    task_uuid  = 1,
    bucket_id  = 2,
    status     = 3,
    created    = 4,
    priority   = 5,
    ttl        = 6,
    next_event = 7,
    data       = 8
}

-- FIBERS METHODs --

local function fiber_iteration(tube_name, processed)
    local curr = time()
    local task = nil
    local estimated = TIMEOUT_INFINITY

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

function tube.create(args)
    local space_options = {}

    local if_not_exists = args.options.if_not_exists or false

    space_options.if_not_exists = if_not_exists
    space_options.temporary = args.options.temporary or false
    space_options.engine = args.options.engine or 'memtx'

    space_options.format = {
        { 'task_uuid',  'string'   },
        { 'bucket_id',  'unsigned' },
        { 'status',     'string'   },
        { 'created',    'unsigned' },
        { 'priority',   'unsigned' },
        { 'ttl',        'unsigned' },
        { 'next_event', 'unsigned' },
        { 'data',       '*'        }
    }

    local space = box.schema.space.create(args.name, space_options)

    space:create_index('task_uuid', {
        type = 'hash',
        parts = {
            index.task_uuid, 'string'
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

function tube.get_task(args)
    -- getting task without change state --
    local task = box.space[args.tube_name].index.status:min { state.READY }
    return task
end

-- function tube.get_task_id(args)
--     -- getting task without change state --
--     local max = box.space[args.tube_name].index.task_uuid:max()
--     return max and max[1] + 1 or 0
-- end

function tube.put(args)

    local delay = args.delay or 0
    local status = state.READY
    local ttl = args.ttl or TIMEOUT_INFINITY
    local next_event

    if delay > 0 then
        status = state.DELAYED
        ttl = ttl + delay
        next_event = event_time(delay)
    else
        next_event = event_time(ttl)
    end

    box.begin()
    local task = box.space[args.tube_name]:insert {
        args.task_uuid,         -- task_uuid
        args.bucket_id,         -- bucket_id
        status,                 -- state
        time(),                 -- created
        args.priority or 0,     -- priority
        time(ttl),              -- ttl
        next_event,             -- next_event
        args.data               -- data
    }
    box.commit()
    return task
end

function tube.take(args)
    for _, tuple in
        box.space[args.tube_name].index.status:pairs(nil, {state.READY} ) do
        if tuple ~= nil and tuple[3] == state.READY then
            return box.space[args.tube_name]:update(tuple[1], { {'=', 3, state.TAKEN} })
        end
    end
end

function tube.touch(tube_name, ttr)
    -- error('fifo queue does not support touch')
end

function tube.delete(args)
    box.begin()
    local task = box.space[args.tube_name]:get(args.task_uuid)
    box.space[args.tube_name]:delete(args.task_uuid)
    box.commit()
    if task ~= nil then
        task = task:transform(index.status, 1, state.DONE)
    end
    return task
end

function tube.release(args)
    local task = box.space[args.tube_name]:update(args.task_uuid, { {'=', 3, state.READY} })
    return task
end

function tube.bury(args)
    local task = box.space[args.tube_name]:update(args.id, { {'=', 3, state.BURIED} })
    return task
end

-- unbury several tasks
function tube.kick(tube_name, count)
    for i = 1, count do
        local task = box.space[tube_name].index.status:min {state.BURIED}
        if task == nil then
            return i - 1
        end
        if task[2] ~= state.BURIED then
            return i - 1
        end

        task = box.space[tube_name]:update(task[1], { {'=', 2, state.READY} })
        -- self.on_task_change(task, 'kick')
    end
    return count
end

return tube
