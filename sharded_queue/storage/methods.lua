local fiber = require('fiber')
local stats_storage = require('sharded_queue.stats.storage')

local methods = {
    'statistic',
    'put',
    'take',
    'delete',
    'touch',
    'ack',
    'peek',
    'release',
    'bury',
    'kick',
}

local function init(metrics, tubes)
    for _, method in pairs(methods) do
        local func = function(args)
            args = args or {}
            args.options = tubes:get_options(args.tube_name) or {}

            local tube_name = args.tube_name
            local before = fiber.clock()
            local ok, ret, err = pcall(tubes.call, tubes, tube_name, method, args)
            local latency = fiber.clock() - before

            metrics.observe(latency, tube_name, method, ok and err == nil)

            if not ok then
                error(ret)
            end

            return ret, err
        end

        local global_name = 'tube_' .. method
        rawset(_G, global_name, func)
        box.schema.func.create(global_name, { if_not_exists = true })
    end

    local tube_statistic_func = function(args)
        local before = fiber.clock()
        local ok, ret, err = pcall(stats_storage.get, args.tube_name)
        local latency = fiber.clock() - before

        metrics.observe(latency, args.tube_name, 'statistic', ok and err == nil)

        if not ok then
            error(ret)
        end

        return ret, err
    end

    rawset(_G, 'tube_statistic', tube_statistic_func)
    box.schema.func.create('tube_statistic', { if_not_exists = true })
end

local function get_list()
    return methods
end

return {
    init = init,
    get_list = get_list,
}
