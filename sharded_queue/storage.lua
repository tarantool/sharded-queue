local log = require('log')
local json = require('json')

local cartridge = require('cartridge')

local state = require('sharded_queue.state')
local statistics = require('sharded_queue.statistics')

local DEFAULT_DRIVER = 'sharded_queue.drivers.fifottl'

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
        local driver_name = tube_opts.driver or DEFAULT_DRIVER
        result[tube_name] = get_driver(driver_name)
    end
    return result
end

local function validate_config(conf_new, _)
    local cfg_tubes = conf_new.tubes or {}
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
    return true
end

local function apply_config(cfg, opts)
    if not box.cfg.read_only then
        local cfg_tubes = cfg.tubes or {}

        local existing_tubes = tubes

        tubes = map_tubes(cfg_tubes)

        -- try create tube --
        for tube_name, driver in pairs(tubes) do
            if existing_tubes[tube_name] == nil then
                tubes[tube_name].create({
                    name = tube_name,
                    options = cfg_tubes[tube_name]
                })
                statistics.reset(tube_name)
            end
        end

        -- try drop tube --
        for tube_name, driver in pairs(existing_tubes) do
            if tubes[tube_name] == nil then
                driver.drop(tube_name)
            end
        end
    end
    return true
end

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

local function init(opts)
    if not box.cfg.read_only then
        statistics.init()

        for _, name in pairs(methods) do
            local func = function(args)
                if args == nil then args = {} end
                local cfg_tubes = cartridge.config_get_readonly('tubes') or {}
                args.options = cfg_tubes[args.tube_name] or {}

                local tube_name = args.tube_name
                if tubes[tube_name].method[name] == nil then error(('Method %s not implemented in tube %s'):format(name, tube_name)) end
                return tubes[tube_name].method[name](args)
            end

            local global_name = 'tube_' .. name
            rawset(_G, global_name, func)
            box.schema.func.create(global_name, { if_not_exists = true })
        end

        local tube_statistic_func = function(args)
            return statistics.get(args.tube_name)
        end

        rawset(_G, 'tube_statistic', tube_statistic_func)
        box.schema.func.create('tube_statistic', { if_not_exists = true })
    end
end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,
    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
