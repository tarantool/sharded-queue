local utils = require('sharded_queue.utils')
local queue_driver = require('sharded_queue.driver_fifottl')

local function apply_config(cfg, opts)
    if opts.is_master then
        local cfg_tubes = cfg.tubes or {}
        local box_tubes_name = queue_driver.tubes()

        -- try create tube --
        for tube, tube_opts in pairs(cfg_tubes) do
            if not utils.array_contains(box_tubes_name, tube) then
                queue_driver.create({
                    name = tube,
                    options = tube_opts
                })
            end
        end

        -- try drop tube --
        for _, tube in pairs(box_tubes_name) do
            if cfg_tubes[tube] == nil then
                queue_driver.drop(tube)
            end
        end
    end
    return true
end

local function init(opts)
    if opts.is_master then
        local space_stat = box.schema.space.create('_stat', {
            format = {
                { 'tube_name', 'string'   },
                -- statistic ---------------
                { 'done',      'unsigned' },
                { 'take',      'unsigned' },
                { 'kick',      'unsigned' },
                { 'bury',      'unsigned' },
                { 'put',       'unsigned' },
                { 'delete',    'unsigned' },
                { 'touch',     'unsigned' },
                { 'ack',       'unsigned' },
                { 'release',   'unsigned' },
                -- default options ---------
                { 'ttl',       'unsigned' },
                { 'ttr',       'unsigned' },
                { 'priority',  'unsigned' }
            }
        })

        space_stat:create_index('primary', {
            type = 'HASH',
            parts = {
                1, 'string'
            }
        })

        for name, func in pairs(queue_driver.method) do
            local global_name = 'tube_' .. name
            rawset(_G, global_name, func)

            box.schema.func.create(global_name)
            box.schema.user.grant('guest', 'execute', 'function', global_name)
        end

        --
    end
end

return {
    init = init,
    apply_config = apply_config,
    dependencies = {
        'cluster.roles.vshard-storage',
    },
}
