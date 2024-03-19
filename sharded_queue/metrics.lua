---- Module with metrics helper code.
local is_metrics_package, metrics = pcall(require, "metrics")

local function create_collectors(self, role)
    local role_full = "sharded_queue." .. role
    local collectors = {}

    -- Unique names help to avoid clashes on instances with both roles.
    collectors.api_stats = {
        collector = metrics.summary(
            string.format("tnt_sharded_queue_%s_role_stats", role),
            string.format("sharded_queue's number of %s API calls", role_full),
            {[0.5]=0.01, [0.95]=0.01, [0.99]=0.01},
            {max_age_time = 60, age_buckets_count = 5}
        ),
        values = {},
    }

    collectors.calls = {
        collector = metrics.counter(
            string.format("tnt_sharded_queue_%s_statistics_calls_total", role),
            string.format("sharded_queue's number of calls on %s", role_full)
        ),
        values = {},
    }
    collectors.tasks = {
        collector = metrics.gauge(
            string.format("tnt_sharded_queue_%s_statistics_tasks", role),
            string.format("sharded_queue's number of task states on %s",
                role_full)
        ),
    }
    self.collectors = collectors
end

local function enable(self, role, tubes, get_statistic_callback)
    -- Drop all collectors and a callback.
    self:disable(self)

    -- Set all collectors and the callback.
    create_collectors(self, role)
    local callback = function()
        for tube_name, _ in pairs(tubes) do
            local statistics = get_statistic_callback(tube_name)

            if statistics ~= nil then
                local collectors = self.collectors
                if collectors.calls.values[tube_name] == nil then
                    collectors.calls.values[tube_name] = {}
                end

                for k, v in pairs(statistics.calls) do
                    local prev = collectors.calls.values[tube_name][k] or 0
                    local inc = v - prev
                    collectors.calls.collector:inc(inc, {
                        name = tube_name,
                        state = k,
                    })
                    collectors.calls.values[tube_name][k] = v
                end
                for k, v in pairs(statistics.tasks) do
                    collectors.tasks.collector:set(v, {
                        name = tube_name,
                        state = k,
                    })
                end
            end
        end
    end

    metrics.register_callback(callback)
    self.callback = callback
    return true
end

local function disable(self)
    if self.callback then
        metrics.unregister_callback(self.callback)
    end
    self.callback = nil

    if self.collectors then
        for _, c in pairs(self.collectors) do
            metrics.registry:unregister(c.collector)
        end
    end
    self.collectors = nil
end

local function observe(self, latency, tube, method, ok)
    if self.collectors ~= nil then
        local status = ok and 'ok' or 'error'
        self.collectors.api_stats.collector:observe(
            latency, {name = tube, method = method, status = status})
    end
end

local mt = {
    __index = {
        enable = enable,
        disable = disable,
        observe = observe,
    }
}

local function is_v_0_11_installed()
    if not is_metrics_package or metrics.unregister_callback == nil then
        return false
    end
    local counter = require('metrics.collectors.counter')
    return counter.remove and true or false
end

local function init(stash)
    setmetatable(stash, mt)
    return stash
end

return {
    is_supported = is_v_0_11_installed,
    init = init,
}
