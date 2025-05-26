---- Module used to store storage-specific statistics.

local state = require('sharded_queue.state')
local vshard_utils = require('sharded_queue.storage.vshard_utils')

local statistics = {}

local actions = {
    done = 2,
    take = 3,
    kick = 4,
    bury = 5,
    put = 6,
    delete = 7,
    touch = 8,
    ack = 9,
    release = 10,
    truncate = 11,
}

function statistics.init()
    local user = vshard_utils.get_this_replica_user() or 'guest'
    local space_stat = box.schema.space.create('_queue_statistics',
        { if_not_exists = true })
    space_stat:format({
        { 'tube_name', 'string' },
        { 'done', 'unsigned' },
        { 'take', 'unsigned' },
        { 'kick', 'unsigned' },
        { 'bury', 'unsigned' },
        { 'put', 'unsigned' },
        { 'delete', 'unsigned' },
        { 'touch', 'unsigned' },
        { 'ack', 'unsigned' },
        { 'release', 'unsigned' },
        { 'truncate', 'unsigned' },
    })

    space_stat:create_index('primary', {
        type = 'HASH',
        parts = {
            1, 'string'
        },
        if_not_exists = true
    })

    box.schema.user.grant(user, 'read,write', 'space', '_queue_statistics',
        {if_not_exists = true})
end

function statistics.update(tube_name, stat_name, operation, value)
    if actions[stat_name] == nil then return end

    box.space._queue_statistics:update(tube_name,
        { { operation, actions[stat_name], value } })
end

function statistics.reset(tube_name)
    local default_stat = box.space._queue_statistics:frommap({
        tube_name = tube_name,
        done = 0,
        take = 0,
        kick = 0,
        bury = 0,
        put = 0,
        delete = 0,
        touch = 0,
        ack = 0,
        release = 0,
        truncate = 0,
    })
    box.space._queue_statistics:replace(default_stat)
end

function statistics.get_states(tube_name)
    local stat = {}
    if box.space[tube_name].index.status ~= nil then
        for name, value in pairs(state) do
            local count = box.space[tube_name].index.status:count(value)
            stat[name:lower()] = count
        end
    end
    stat.total = box.space[tube_name]:count()
    return stat
end

function statistics.get_actions(tube_name)
    local stat = box.space._queue_statistics:get(tube_name)
    if stat ~= nil then
        stat = stat:tomap({ names_only = true })
        stat.tube_name = nil
    end

    return stat
end

function statistics.get(tube_name)
    if not box.space[tube_name] then return nil end
    local stat = {
        tasks = statistics.get_states(tube_name) or {},
        calls = statistics.get_actions(tube_name) or {}
    }

    -- for backward compatibility with tarantool/queue
    stat.tasks.done = stat.calls.done

    return stat
end

return statistics
