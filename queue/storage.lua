local cluster = require('cluster')
local checks = require('checks')
local log = require('log')

package.path = package.path .. ";queue/?.lua"
local queue_driver = require('driver_fifottl')

local function apply_config(cfg, opts)
    if opts.is_master then
        for _, t in pairs(cfg.tubes or {}) do
            if not queue_driver.check(t.name) then
                queue_driver.create(t)
            end
        end
    end
    return true
end

local function init(opts)
    if opts.is_master then
        --
        box.schema.user.grant('guest',
            'cread,write,execute',
            'universe',
            nil, { if_not_exists = true })
        --
        box.schema.func.create('tube_create',  {if_not_exists = true})
        box.schema.func.create('tube_put',     {if_not_exists = true})
        box.schema.func.create('tube_take',    {if_not_exists = true})
        box.schema.func.create('tube_delete',  {if_not_exists = true})
        box.schema.func.create('tube_release', {if_not_exists = true})
        --
        box.schema.user.grant('guest', 'execute', 'function', 'tube_create',  {if_not_exists = true})
        box.schema.user.grant('guest', 'execute', 'function', 'tube_put',     {if_not_exists = true})
        box.schema.user.grant('guest', 'execute', 'function', 'tube_take',    {if_not_exists = true})
        box.schema.user.grant('guest', 'execute', 'function', 'tube_delete',  {if_not_exists = true})
        box.schema.user.grant('guest', 'execute', 'function', 'tube_release', {if_not_exists = true})
        --
    end
    --
    rawset(_G, 'tube_create',  queue_driver.create)
    rawset(_G, 'tube_put',     queue_driver.put)
    rawset(_G, 'tube_take',    queue_driver.take)
    rawset(_G, 'tube_delete',  queue_driver.delete)
    rawset(_G, 'tube_release', queue_driver.release)
    --
end

return {
    init = init,
    apply_config = apply_config,
    dependencies = {
        'cluster.roles.vshard-storage',
    },
}
