local log = require('log')
local json = require('json')

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

local function apply_config(cfg, opts)
    if opts.is_master then
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
    if opts.is_master then
        local space_stat = box.schema.space.create('_stat', {
            format = {
                { 'tube_name', 'string' },
                -- statistic ---------------
                { 'done', 'unsigned' },
                { 'take', 'unsigned' },
                { 'kick', 'unsigned' },
                { 'bury', 'unsigned' },
                { 'put', 'unsigned' },
                { 'delete', 'unsigned' },
                { 'touch', 'unsigned' },
                { 'ack', 'unsigned' },
                { 'release', 'unsigned' },
                -- default options ---------
                { 'ttl', 'unsigned' },
                { 'ttr', 'unsigned' },
                { 'priority', 'unsigned' }
            },
            if_not_exists = true
        })

        space_stat:create_index('primary', {
            type = 'HASH',
            parts = {
                1, 'string'
            },
            if_not_exists = true
        })
        for _, name in pairs(methods) do
            local func = function(args)
                local tube_name = args.tube_name
                return tubes[tube_name].method[name](args)
            end

            local global_name = 'tube_' .. name
            rawset(_G, global_name, func)
            box.schema.func.create(global_name, { if_not_exists = true })
        end
    end
end

return {
    init = init,
    apply_config = apply_config,
    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
