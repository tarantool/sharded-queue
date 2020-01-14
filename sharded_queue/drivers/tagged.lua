local state = require('sharded_queue.state')
local utils = require('sharded_queue.utils')
local log = require('log') -- luacheck: ignore

local method = {}

local function tube_create(opts)
    local space_opts = {}
    local if_not_exists = opts.if_not_exists or true
    space_opts.temporary = opts.temporary or false
    space_opts.if_not_exists = if_not_exists
    space_opts.engine = opts.engine or 'memtx'
    space_opts.format = {
        { name = 'task_id', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
        { name = 'status', type = 'string' },
        { name = 'data', type = '*' },
        { name = 'index', type = 'unsigned' },
        { name = 'tags', type = 'array'}
    }

    local space = box.schema.create_space(opts.name, space_opts)
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

    return space
end

local function tube_drop(tube_name)
    box.space._stat:delete(tube_name)
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

-- put task in space
function method.put(args)
    local idx = get_index(args)
    local task_id = utils.pack_task_id(args.bucket_id, args.bucket_count, idx)
    local task = get_space(args):insert { task_id, args.bucket_id, state.READY, args.data, idx, args.tags or {} }
    return normalize_task(task)
end

local function tags_are_available(task_tags, take_tags)
    if #task_tags == 0 then
        return #take_tags == 0
    end

    local task_tags_set, take_tags_set = {}, {}
    for _, tag in ipairs(task_tags) do
        task_tags_set[tag] = true
    end

    for _, tag in ipairs(take_tags) do
        take_tags_set[tag] = true
    end

    for tag, _ in pairs(task_tags_set) do
        if take_tags_set[tag] == nil then
            return false
        end
    end
    return true
end

-- take task
function method.take(args)
    for _, task in get_space(args).index.status:pairs({ state.READY }) do
        if task ~= nil and task.status == state.READY and tags_are_available(task.tags, args.tags or {}) then
            task = get_space(args):update(task.task_id, { { '=', 3, state.TAKEN } })
            return normalize_task(task)
        end
    end
end

function method.ack(args)
    local task = box.space[args.tube_name]:get(args.task_id)
    box.space[args.tube_name]:delete(args.task_id)
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
    end

    return normalize_task(task)
end

-- touch task
function method.touch(args)
    error('tagged queue does not support touch')
end

-- delete task
function method.delete(args)
    local task = get_space(args):get(args.task_id)
    get_space(args):delete(args.task_id)
    if task ~= nil then
        task = task:tomap()
        task.status = state.DONE
    end
    return normalize_task(task)
end

-- release task
function method.release(args)
    local task = get_space(args):update(args.task_id, { { '=', 3, state.READY } })
    return normalize_task(task)
end

-- bury task
function method.bury(args)
    local task = get_space(args):update(args.task_id, { { '=', 3, state.BURIED } })
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

        task = get_space(args):update(task.task_id, { { '=', 3, state.READY } })
    end
    return args.count
end

-- peek task
function method.peek(args)
    return normalize_task(get_space(args):get { args.task_id })
end

function method.truncate(self)
    self.space:truncate()
end

return {
    create = tube_create,
    drop = tube_drop,
    method = method
}
