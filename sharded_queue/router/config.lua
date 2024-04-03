local metrics = require('sharded_queue.metrics')
local stash = require('sharded_queue.stash')

local stash_names = {
    config = '__sharded_queue_router_config',
}
stash.setup(stash_names)

local config = stash.get(stash_names.config)

if config.metrics == nil then
    config.metrics = true
end

if config.metrics then
    config.metrics = metrics.is_supported()
end

return config
