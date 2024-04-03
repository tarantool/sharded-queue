local is_cartridge_package, cartridge = pcall(require, 'cartridge')
local vshard = require('vshard')
local tube = require('sharded_queue.router.tube')
local utils = require('sharded_queue.utils')

local queue_global = {
    tube = {},
}

function queue_global.statistics(tube_name)
    if not tube_name then
        return
    end

    local stats_collection, err = vshard.router.map_callrw('tube_statistic',
        {{ tube_name = tube_name }})
    if err ~= nil then
        return nil, err
    end

    if type(stats_collection) ~= 'table' then
        return nil, 'No stats retrieved'
    end

    if next(stats_collection) == nil then
        return nil
    end

    local stat = { tasks = {}, calls = {} }
    for _, replicaset_stats in pairs(stats_collection) do
        if type(replicaset_stats) ~= 'table' or next(replicaset_stats) == nil then
            return nil, 'Invalid stats'
        end

        for name, count in pairs(replicaset_stats[1].tasks) do
            stat.tasks[name] = (stat.tasks[name] or 0) + count
        end
        for name, count in pairs(replicaset_stats[1].calls) do
            stat.calls[name] = (stat.calls[name] or 0) + count
        end
    end

    return stat
end

-- The Tarantool 3.0 does not support to update dinamically a configuration, so
-- a user must update the configuration by itself.
if is_cartridge_package then
    local function validate_options(options)
        if not options then return true end

        if options.wait_factor then
            if type(options.wait_factor) ~= 'number'
                or options.wait_factor < 1
            then
                return false, "wait_factor must be number greater than or equal to 1"
            end
        end

        local _, err = utils.normalize.log_request(options.log_request)
        if err then
            return false, err
        end

        if options.wait_max ~= nil then
            local err
            options.wait_max, err = utils.normalize.wait_max(options.wait_max)
            if err ~= nil then
                return false, err
            end
        end

        return true
    end

    queue_global.create_tube = function(tube_name, options)
        require('log').info("CREATE TUBE")
        local tubes = cartridge.config_get_deepcopy('tubes') or {}

        if tube_name == 'cfg' then
            error('a tube name "cfg" is reserved')
        end

        if tubes[tube_name] ~= nil then
            return nil
        end

        local ok , err = validate_options(options)
        if not ok then error(err) end

        options = table.deepcopy(options or {})
        if options.priority == nil and options.pri ~= nil then
            options.priority = options.pri
        end

        tubes[tube_name] = options
        ok, err = cartridge.config_patch_clusterwide({ tubes = tubes })
        if not ok then
            error(err)
        end

        return queue_global.tube[tube_name]
    end
end

local function export_globals()
    rawset(_G, 'queue', queue_global)
end

local function add(name, metrics, options)
    queue_global.tube[name] = tube.new(name, metrics, options)
end

local function call(tube, action, ...)
    if queue_global.tube[tube] == nil then
        return nil, string.format('No queue "%s" initialized yet', name)
    end
    if queue_global.tube[tube][action] == nil then
        return nil, string.format('Queue %s has not action %s', tube, action)
    end
    return queue_global.tube[tube][action](queue_global.tube[tube], ...)
end

local function map()
    return queue_global.tube
end

local function remove(tube)
    if queue_global.tube[tube] ~= nil then
        setmetatable(queue_global.tube[tube], nil)
        queue_global.tube[tube] = nil
    end
end

return {
    export_globals = export_globals,
    add = add,
    call = call,
    map = map,
    statistics = queue_global.statistics,
    remove = remove,
}
