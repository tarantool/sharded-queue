local cluster = require('cluster')
local checks = require('checks')
local log = require('log')

local utils = require('shared_queue.utils')
local queue_driver = require('shared_queue.driver_fifottl')

local function apply_config(cfg, opts)
    if opts.is_master then
        local cfg_tubes = cfg.tubes or {}
        local box_tubes_name = queue_driver.tubes()

        -- try create tube --
        for tube, opts in pairs(cfg_tubes) do
            if not utils.array_contains(box_tubes_name, tube) then
                queue_driver.create({
                    name = tube,
                    options = opts
                })
                return true
            end
        end

        -- try drop tube --
        for _, tube in pairs(box_tubes_name) do
            if cfg_tubes[tube] == nil then
                queue_driver.drop(tube)
                return true
            end
        end
    end
end

local function init(opts)
    if opts.is_master then
        --
        box.schema.user.grant('guest',
            'read,write',
            'universe',
            nil, { if_not_exists = true })
        --

        local space_stat = box.schema.space.create('_stat', {
            format = {
                { 'tube_name', 'string'   },
                { 'done',      'unsigned' },
                { 'take',      'unsigned' },
                { 'kick',      'unsigned' },
                { 'bury',      'unsigned' },
                { 'put',       'unsigned' },
                { 'delete',    'unsigned' },
                { 'touch',     'unsigned' },
                { 'ask',       'unsigned' },
                { 'release',   'unsigned' }
            }
        })
    
        space_stat:create_index('primary', {
            type = 'HASH',
            parts = {
                1, 'string'
            }
        })

        rawset(_G, 'tube_put', queue_driver.put)
        box.schema.func.create('tube_put')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_put')
        
        rawset(_G, 'tube_take', queue_driver.take)
        box.schema.func.create('tube_take')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_take')
        
        rawset(_G, 'tube_delete', queue_driver.delete)
        box.schema.func.create('tube_delete')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_delete')
        
        rawset(_G, 'tube_release', queue_driver.release)
        box.schema.func.create('tube_release')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_release')

        rawset(_G, 'tube_touch', queue_driver.touch)
        box.schema.func.create('tube_touch')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_touch')

        rawset(_G, 'tube_ask', queue_driver.ask)
        box.schema.func.create('tube_ask')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_ask')

        rawset(_G, 'tube_bury', queue_driver.bury)
        box.schema.func.create('tube_bury')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_bury')

        rawset(_G, 'tube_kick', queue_driver.kick)
        box.schema.func.create('tube_kick')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_kick')

        rawset(_G, 'tube_statistic', queue_driver.statistic)
        box.schema.func.create('tube_statistic')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_statistic')
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
