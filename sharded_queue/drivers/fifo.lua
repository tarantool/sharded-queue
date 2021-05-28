local state = require('sharded_queue.state')
local utils = require('sharded_queue.utils')
local log = require('log') -- luacheck: ignore
local statistics = require('sharded_queue.statistics')
local time  = require('sharded_queue.time')
local fiber = require('fiber')

local function update_stat(tube_name, name)
    statistics.update(tube_name, name, '+', 1)
end

local dedup_index = {
    deduplication_id  = 1,
    created           = 2,
    bucket_id         = 3
}

local function fiber_iteration(tube_name)
    local cur  = time.cur()
    local timeout = time.DEDUPLICATION_TIME
    -- delete old tasks
    local cnt = 0
    for _, tuple in box.space[tube_name].index.created:pairs({cur - timeout}, {iterator = box.index.LT, limit = 1000}) do
        cnt = cnt + 1
        box.space[tube_name]:delete(tuple[dedup_index.deduplication_id])
    end
    timeout = time.sec(timeout)

    local task = box.space[tube_name].index.created:min()
    if task ~= nil then
        local e = time.sec(tonumber(task[dedup_index.created] - cur + time.DEDUPLICATION_TIME))
        timeout = e < timeout and e or timeout
    end

    fiber.sleep(timeout)

    return cnt
end

local function fiber_common(tube_name)
    fiber.name(tube_name)

    while true do
        if not box.cfg.read_only then
            local ok, ret = pcall(fiber_iteration, tube_name)
            if not ok and not (ret.code == box.error.READONLY) then
                return 1
            end
        else
            fiber.sleep(0.1)
        end
    end
end

local method = {}
local deduplication_suffix = '_deduplication'

local function tube_create(args)
    local space_opts = {}
    local if_not_exists = args.if_not_exists or true
    space_opts.temporary = args.temporary or false
    space_opts.if_not_exists = if_not_exists
    space_opts.engine = args.engine or 'memtx'
    space_opts.format = {
        { name = 'task_id', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
        { name = 'status', type = 'string' },
        { name = 'data', type = '*' },
        { name = 'index', type = 'unsigned' }
    }

    local space = box.schema.create_space(args.name, space_opts)
    space:create_index('task_id', {
        type = 'tree',
        parts = { 'task_id' },
        if_not_exists = if_not_exists
    })
    space:create_index('status', {
        type = 'tree',
        parts = { 'status', 'task_id' },
        if_not_exists = if_not_exists
    })

    space:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = if_not_exists
    })

    space:create_index('idx', {
        type = 'tree',
        parts = { 'bucket_id', 'index' },
        if_not_exists = if_not_exists
    })

    if args.options.content_based_deduplication == true then
        local deduplication_opts = {}
        deduplication_opts.temporary = args.options.temporary or false
        deduplication_opts.if_not_exists = if_not_exists
        deduplication_opts.engine = args.options.engine or 'memtx'
        deduplication_opts.format = {
            { name = 'deduplication_id', type = 'string' },
            { name = 'created', type = 'unsigned' },
            { name = 'bucket_id', type = 'unsigned' }
        }
        local deduplication = box.schema.create_space(args.name .. deduplication_suffix, deduplication_opts)
        deduplication:create_index('deduplication_id', {
            type = 'tree',
            parts = { 'deduplication_id' },
            unique = true,
            if_not_exists = if_not_exists
        })
        deduplication:create_index('created', {
            type = 'tree',
            parts = { 'created' },
            unique = false,
            if_not_exists = if_not_exists
        })
        deduplication:create_index('bucket_id', {
            type = 'tree',
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = if_not_exists
        })

        -- run fiber for deduplication event
        fiber.create(fiber_common, args.name .. deduplication_suffix)
    end

    return space
end

local function get_space_by_name(name)
    return box.space[name]
end

local function get_space(args)
    return get_space_by_name(args.tube_name)
end

local function get_deduplication_space(args)
    return get_space_by_name(args.tube_name .. deduplication_suffix)
end

local function tube_drop(tube_name)
    box.space[tube_name]:drop()
    local space = get_space_by_name(tube_name .. deduplication_suffix)
    if space ~= nil then
        space:drop()
    end
end

local function get_index(args)
    local task = get_space(args).index.idx:max { args.bucket_id }
    if not task or task.bucket_id ~= args.bucket_id then
        return 1
    else
        return task.index + 1
    end
end

local function normalize_task(task)
    if task == nil then return nil end
    return { task.task_id, task.status, task.data }
end

-- put task in space
function method.put(args)
    if args.message_deduplication_id ~= nil or args.options.message_deduplication_id ~= nil then
        local key = args.message_deduplication_id or args.options.message_deduplication_id
        local space = get_deduplication_space(args)
        if space ~= nil then
            space:insert {key, time.cur(), args.bucket_id}
        end
    end
    local idx = get_index(args)
    local task_id = utils.pack_task_id(args.bucket_id, args.bucket_count, idx)
    local task = get_space(args):insert { task_id, args.bucket_id, state.READY, args.data, idx }
    update_stat(args.tube_name, 'put')
    return normalize_task(task)
end

-- take task
function method.take(args)
    local task = get_space(args).index.status:min { state.READY }
    if task ~= nil and task[3] == state.READY then
        task = get_space(args):update(task.task_id, { { '=', 3, state.TAKEN } })
        update_stat(args.tube_name, 'take')
        return normalize_task(task)
    end
end

function method.ack(args)
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
        update_stat(args.tube_name, 'ack')
        update_stat(args.tube_name, 'done')
    end

    return normalize_task(task)
end

-- touch task
function method.touch(args)
    error('fifo queue does not support touch')
end

-- delete task
function method.delete(args)
    local task = get_space(args):get(args.task_id)
    get_space(args):delete(args.task_id)
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
        update_stat(args.tube_name, 'delete')
        update_stat(args.tube_name, 'done')
    end
    return normalize_task(task)
end

-- release task
function method.release(args)
    local task = get_space(args):update(args.task_id, { { '=', 3, state.READY } })
    update_stat(args.tube_name, 'release')
    return normalize_task(task)
end

-- bury task
function method.bury(args)
    local task = get_space(args):update(args.task_id, { { '=', 3, state.BURIED } })
    update_stat(args.tube_name, 'bury')
    return normalize_task(task)
end

-- unbury several tasks
function method.kick(args)
    for i = 1, args.count do
        local task = get_space(args).index.status:min { state.BURIED }
        if task == nil then
            return i - 1
        end
        if task[2] ~= state.BURIED then
            return i - 1
        end

        task = get_space(args):update(task[1], { { '=', 3, state.READY } })
        update_stat(args.tube_name, 'kick')
    end
    return args.count
end

-- peek task
function method.peek(args)
    return normalize_task(get_space(args):get { args.task_id })
end

return {
    create = tube_create,
    drop = tube_drop,
    method = method
}
