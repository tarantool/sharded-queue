local fiber = require('fiber')
local json = require('json')
local log = require('log')

local cartridge = require('cartridge')

local metrics = require('sharded_queue.metrics')
local stash = require('sharded_queue.stash')
local state = require('sharded_queue.state')
local stats_storage = require('sharded_queue.stats.storage')
local utils = require('sharded_queue.utils')

local DEFAULT_DRIVER = 'sharded_queue.drivers.fifottl'

local stash_names = {
    cfg = '__sharded_queue_storage_cfg',
    metrics_stats = '__sharded_queue_storage_metrics_stats',
}
stash.setup(stash_names)

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

local storage = {
    cfg = stash.get(stash_names.cfg),
    metrics_stats = metrics.init(stash.get(stash_names.metrics_stats)),
}

if storage.cfg.metrics == nil then
    storage.cfg.metrics = true
end

if storage.cfg.metrics then
    storage.cfg.metrics = metrics.is_supported()
end

local queue_drivers = {}
local function get_driver(driver_name)
    if queue_drivers[driver_name] == nil then
        queue_drivers[driver_name] = require(driver_name)
    end
    return queue_drivers[driver_name]
end

local tubes = {}

local function map_tubes(cfg_tubes)
    local result = {}
    for tube_name, tube_opts in pairs(cfg_tubes) do
        if tube_name['cfg'] ~= nil or tube_opts.enable == nil then
            -- do not add 'cfg' as a tube
            local driver_name = tube_opts.driver or DEFAULT_DRIVER
            result[tube_name] = get_driver(driver_name)
        end
    end
    return result
end

local function metrics_enable()
    local get_statistic = function(tube)
        return stats_storage.get(tube)
    end

    storage.metrics_stats:enable('storage', tubes, get_statistic)
end

local function metrics_disable()
    storage.metrics_stats:disable()
end

local function validate_config(cfg)
    local cfg_tubes = cfg.tubes or {}
    for tube_name, tube_opts in pairs(cfg_tubes) do
        if tube_opts.driver ~= nil then
            if type('tube_opts.driver') ~= 'string' then
                return nil, 'Driver name must be a valid module name for tube' .. tube_name
            end
            local ok, _ = pcall(require, tube_opts.driver)
            if not ok then
                return nil, ('Driver %s could not be loaded for tube %s'):format(tube_opts.driver, tube_name)
            end
        end
    end

    return utils.validate_config_cfg(cfg)
end

local function apply_config(cfg, opts)
    if opts.is_master then
        stats_storage.init()

        local cfg_tubes = cfg.tubes or {}
        if cfg_tubes['cfg'] ~= nil then
            local options = cfg_tubes['cfg']
            if options.metrics ~= nil then
                storage.cfg.metrics = options.metrics and true or false
            end
        end

        local existing_tubes = tubes

        tubes = map_tubes(cfg_tubes)

        -- try create tube --
        for tube_name, driver in pairs(tubes) do
            if existing_tubes[tube_name] == nil then
                tubes[tube_name].create({
                    name = tube_name,
                    options = cfg_tubes[tube_name]
                })
                stats_storage.reset(tube_name)
            end
        end

        -- try drop tube --
        for tube_name, driver in pairs(existing_tubes) do
            if tubes[tube_name] == nil then
                driver.drop(tube_name)
            end
        end

        -- register tube methods --
        for _, name in pairs(methods) do
            local func = function(args)
                if args == nil then args = {} end
                args.options = cfg_tubes[args.tube_name] or {}

                local tube_name = args.tube_name
                if tubes[tube_name].method[name] == nil then error(('Method %s not implemented in tube %s'):format(name, tube_name)) end

                local before = fiber.clock()
                local ok, ret, err = pcall(tubes[tube_name].method[name], args)
                local after = fiber.clock()

                if storage.cfg.metrics then
                    storage.metrics_stats:observe(after - before,
                        tube_name, name, ok and err == nil)
                end

                if not ok then
                    error(ret)
                end

                return ret, err
            end

            local global_name = 'tube_' .. name
            rawset(_G, global_name, func)
            box.schema.func.create(global_name, { if_not_exists = true })
        end

        local tube_statistic_func = function(args)
            local before = fiber.clock()
            local ok, ret, err = pcall(stats_storage.get, args.tube_name)
            local after = fiber.clock()
            if storage.cfg.metrics then
                storage.metrics_stats:observe(after - before,
                    args.tube_name, 'statistic', ok and err == nil)
            end

            if not ok then
                error(ret)
            end

            return ret, err
        end

        rawset(_G, 'tube_statistic', tube_statistic_func)
        box.schema.func.create('tube_statistic', { if_not_exists = true })
    end

    if storage.cfg.metrics then
        metrics_enable()
    else
        metrics_disable()
    end

    return true
end

local function init(opts)

end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,
    _VERSION = require('sharded_queue.version'),

    dependencies = {
        'cartridge.roles.vshard-storage',
    },

    __private = {
        methods = methods,
    }
}
