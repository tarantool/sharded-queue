local state = require('sharded_queue.state')
local utils = require('sharded_queue.utils')
local log = require('log') -- luacheck: ignore
local stats = require('sharded_queue.stats.storage')
local vshard_utils = require('sharded_queue.storage.vshard_utils')
local consts = require('sharded_queue.consts')

local function update_stat(tube_name, name)
    stats.update(tube_name, name, '+', 1)
end

local index = {
    task_id         = 1,
    bucket_id       = 2,
    status          = 3,
    data            = 4,
    index           = 5,
    release_count   = 6,
}

local method = {}

local function tube_create(args)
    local user = vshard_utils.get_this_replica_user() or 'guest'
    local space_options = {}
    local if_not_exists = args.options.if_not_exists or true
    space_options.if_not_exists = if_not_exists
    space_options.temporary = args.options.temporary or false
    space_options.engine = args.options.engine or 'memtx'
    space_options.format = {
        { name = 'task_id', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
        { name = 'status', type = 'string' },
        { name = 'data', type = '*' },
        { name = 'index', type = 'unsigned' },
        { name = 'release_count', type = 'unsigned' }
    }

    local space = box.schema.create_space(args.name, space_options)
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

    box.schema.user.grant(user, 'read,write', 'space', args.name,
        {if_not_exists = true})

    return space
end

local function tube_drop(tube_name)
    box.space[tube_name]:drop()
end

local function get_space(args)
    return box.space[args.tube_name]
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

local function put_in_dlq(args, task)
    -- setup dead letter queue args
    local dlq_args = {}
    dlq_args.tube_name = args.tube_name .. consts.DLQ_SUFFIX
    dlq_args.data = task.data
    dlq_args.bucket_id = task.bucket_id
    dlq_args.task_id = task.task_id

    local idx = get_index(dlq_args)

    get_space(dlq_args):insert {
        dlq_args.task_id,
        dlq_args.bucket_id,
        state.READY,
        dlq_args.data,
        idx,
        0
    }

    update_stat(dlq_args.tube_name, 'put')
end

-- put task in space
function method.put(args)
    local task = utils.atomic(function()
        local idx = get_index(args)
        local task_id = utils.pack_task_id(
            args.bucket_id,
            args.bucket_count,
            idx)

        return get_space(args):insert {
            task_id,
            args.bucket_id,
            state.READY,
            args.data,
            idx,
            0
        }
    end)

    update_stat(args.tube_name, 'put')
    return normalize_task(task)
end

-- take task
function method.take(args)
    local task = utils.atomic(function()
        local task = get_space(args).index.status:min { state.READY }
        if task == nil or task[index.status] ~= state.READY then
            return
        end
        return get_space(args):update(task.task_id, { { '=', index.status, state.TAKEN } })
    end)
    if task == nil then return end

    update_stat(args.tube_name, 'take')
    return normalize_task(task)
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
    local release_limit = args.options.release_limit or -1
    local deleted = false
    local release_limit_policy =
        args.options.release_limit_policy or consts.RELEASE_LIMIT_POLICY.DELETE

    local task = utils.atomic(function()
        local task = get_space(args):update(args.task_id, {
            { '=', index.status, state.READY },
            { '+', index.release_count, 1 },
        })
        if task ~= nil and release_limit > 0 then
            if task.release_count >= release_limit then
                get_space(args):delete(args.task_id)
                if release_limit_policy == consts.RELEASE_LIMIT_POLICY.DLQ then
                    put_in_dlq(args, task)
                end
                deleted = true
            end
        end
        return task
    end)

    update_stat(args.tube_name, 'release')

    if deleted then
        task = task:tomap()
        task.status = state.DONE
        update_stat(args.tube_name, 'delete')
        update_stat(args.tube_name, 'done')
    end

    return normalize_task(task)
end

-- bury task
function method.bury(args)
    local task = get_space(args):update(args.task_id, { { '=', index.status, state.BURIED } })
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
        if task[index.status] ~= state.BURIED then
            return i - 1
        end

        task = get_space(args):update(task[index.task_id], { { '=', index.status, state.READY } })
        update_stat(args.tube_name, 'kick')
    end
    return args.count
end

-- peek task
function method.peek(args)
    return normalize_task(get_space(args):get { args.task_id })
end

function method.truncate(args)
    update_stat(args.tube_name, "truncate")
    return get_space(args):truncate()
end

return {
    create = tube_create,
    drop = tube_drop,
    method = method
}
