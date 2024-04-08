local metrics = require('sharded_queue.metrics')
local stash = require('sharded_queue.stash')
local stats_storage = require('sharded_queue.stats.storage')

local stash_names = {
    metrics_stats = '__sharded_queue_router_metrics_stats',
}
stash.setup(stash_names)

local metrics_stats = metrics.init(stash.get(stash_names.metrics_stats))

local function enable(queue)
    local get_statistic = function(tube)
        return queue.statistics(tube)
    end
    metrics_stats:enable('router', queue.map(), get_statistic)
end

local function observe(latency, tube, method, ok)
    metrics_stats:observe(latency, tube, method, ok)
end

local function disable()
    metrics_stats:disable()
end

return {
    enable = enable,
    observe = observe,
    disable = disable,
}
