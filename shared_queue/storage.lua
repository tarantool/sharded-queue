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
                -- statistic ---------------
                { 'done',      'unsigned' },
                { 'take',      'unsigned' },
                { 'kick',      'unsigned' },
                { 'bury',      'unsigned' },
                { 'put',       'unsigned' },
                { 'delete',    'unsigned' },
                { 'touch',     'unsigned' },
                { 'ask',       'unsigned' },
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
