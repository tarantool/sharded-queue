local cluster = require('cluster')
local checks = require('checks')
local tube = require('tube_fifo')

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
    rawset(_G, 'tube_create',  tube.create)
    rawset(_G, 'tube_put',     tube.put)
    rawset(_G, 'tube_take',    tube.take)
    rawset(_G, 'tube_delete',  tube.delete)
    rawset(_G, 'tube_release', tube.release)
    --
end

return {
    init = init,
    dependencies = {
        'cluster.roles.vshard-storage',
    },
}
