local t = require('luatest')
local g = t.group('metrics_test')

local config = require('test.helper.config')
local json = require('json')
local utils = require('test.helper.utils')

g.before_all(function()
    g.queue_conn = config.cluster:server('queue-router').net_box
    g.cfg = g.queue_conn:eval("return require('sharded_queue.api').cfg")
end)

g.before_each(function()
    t.skip_if(not utils.is_metrics_supported(),
              "metrics >= 0.11.0 is not installed")
    g.queue_conn:eval("require('metrics').clear()")
end)

g.after_each(function()
    g.queue_conn:eval("require('sharded_queue.api').cfg(...)", {g.cfg})
end)

local function filter_metrics(metrics, labels)
    local filtered = {}
    for _, v in pairs(metrics) do
        local found = true
        for label, value in pairs(labels) do
            if not v.label_pairs
                or not v.label_pairs[label]
                or v.label_pairs[label] ~= value then
                found = false
            end
         end
         if found then
             table.insert(filtered, v)
         end
    end
    return filtered
end

local function get_metrics(tube_name, instance)
    local metrics
    local eval = [[
local metrics = require('metrics')
metrics.invoke_callbacks()
return metrics.collect()
]]
    if instance == nil then
        metrics = g.queue_conn:eval(eval)
    else
        metrics = config.cluster:server(instance).net_box:eval(eval)
    end

    for _, v in ipairs(metrics) do
        v.timestamp = nil
    end

    return filter_metrics(metrics, {name = tube_name})
end

local function get_router_metrics(tube_name)
    return get_metrics(tube_name, nil)
end

local function merge_metrics(first, second)
    for _, s in pairs(second) do
        local found = false
        for _, f in pairs(first) do
            if f.metric_name == s.metric_name then
                if json.encode(f.label_pairs) == json.encode(s.label_pairs) then
                    found = true
                    f.value = f.value + s.value
                    break
                end
            end
        end
        if not found then
            table.insert(first, s)
        end
    end
    return first
end

-- Values on storages could be random, so we are interesting in an accumulated
-- value.
local function get_storages_metrics(tube_name)
    local masters = {"queue-storage-1-0", "queue-storage-2-0"}
    local metrics = {}
    for _, instance in ipairs(masters) do
        local instance_metrics = get_metrics(tube_name, instance)
        metrics = merge_metrics(metrics, instance_metrics)
    end
    return metrics
end

local function get_metric(metrics, metric_name)
    local filtered = {}
    for _, v in pairs(metrics) do
        if v.metric_name and v.metric_name == metric_name then
            table.insert(filtered, v)
        end
    end
    return filtered
end

local function assert_metric(metrics, name, label, values, filters)
    local metric = get_metric(metrics, name)
    if filters ~= nil then
        metric = filter_metrics(metric, filters)
    end

    for k, v in pairs(values) do
        local filtered = filter_metrics(metric, {[label] = k})
        json.encode(filtered)
        t.assert_equals(#filtered, 1, label .. "_" .. k)
        t.assert_equals(filtered[1].value, v, label .. "_" .. k)
    end
end

g.test_metrics_api = function()
    local tube_name = 'metrics_api_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local task_count = 64
    for i = 1, task_count do
        g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
            i, { delay = 3 , ttl = 3, ttr = 1}
        })
    end
    t.assert_error(function()
        g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {'invalid'})
    end)

    -- Check metrics on the router.
    local metrics = get_router_metrics(tube_name)
    assert_metric(metrics, "tnt_sharded_queue_api_role_stats_count", "method", {
        put = task_count,
    }, {status = "ok"})
    assert_metric(metrics, "tnt_sharded_queue_api_statistics_calls_total", "state", {
        done = 0,
        take = 0,
        kick = 0,
        bury = 0,
        put = task_count,
        delete = 0,
        touch = 0,
        ack = 0,
        release = 0,
    })
    assert_metric(metrics, "tnt_sharded_queue_api_statistics_tasks", "state", {
        ready = 0,
        taken = 0,
        done = 0,
        buried = 0,
        delayed = task_count,
        total = task_count,
    })
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_api_role_stats"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_api_role_stats_sum"), {})

    -- Check metrics on storages.
    metrics = get_storages_metrics(tube_name)
    assert_metric(metrics, "tnt_sharded_queue_storage_role_stats_count", "method", {
        put = task_count,
    }, {status = "ok"})
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_calls_total", "state", {
        done = 0,
        take = 0,
        kick = 0,
        bury = 0,
        put = task_count,
        delete = 0,
        touch = 0,
        ack = 0,
        release = 0,
    })
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_tasks", "state", {
        ready = 0,
        taken = 0,
        done = 0,
        buried = 0,
        delayed = task_count,
        total = task_count,
    })
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_sum"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_count"), {})
end

g.test_metrics_api_disabled = function()
    local tube_name = 'metrics_api_disabled_test'
    g.queue_conn:eval("require('sharded_queue.api').cfg(...)",
        {{metrics = false}})

    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        1, { delay = 3 , ttl = 3, ttr = 1}
    })
    local metrics = get_router_metrics(tube_name)
    t.assert_equals(metrics, {})

    metrics = get_storages_metrics(tube_name)
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_statistics_calls_total"), {})
    t.assert_equals(get_metric(metrics, "sharded_queue_storage_errors"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_sum"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_count"), {})
end

g.test_metrics_api_disable = function()
    local tube_name = 'metrics_api_disable_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        1, { delay = 3 , ttl = 3, ttr = 1}
    })
    local metrics = get_router_metrics(tube_name)
    assert_metric(metrics, "tnt_sharded_queue_api_statistics_calls_total", "state", {
        put = 1,
    })
    assert_metric(metrics, "tnt_sharded_queue_api_statistics_tasks", "state", {
        delayed = 1,
    })
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_api_role_stats"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_api_role_stats_sum"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_api_role_stats_count"), {})

    metrics = get_storages_metrics(tube_name)
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_calls_total", "state", {
        put = 1,
    })
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_tasks", "state", {
        delayed = 1,
    })
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_sum"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_count"), {})

    g.queue_conn:eval("require('sharded_queue.api').cfg(...)",
        {{metrics = false}})

    metrics = get_router_metrics(tube_name)
    t.assert_equals(metrics, {})

    metrics = get_storages_metrics(tube_name)
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_statistics_calls_total"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_statistics_tasks"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_sum"), {})
    t.assert_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_count"), {})
end

g.test_metrics_storage = function()
    local tube_name = 'metrics_storage_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local storage = config.cluster:server('queue-storage-1-0').net_box
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

    -- Some of them will fail, some of them not - it does not metter. We just
    -- ensure that metrics works on the storage.
    local oks = {}
    local errors = {}
    for _, method in ipairs(methods) do
        local ok, _, err = pcall(function()
            return storage:call("tube_" .. method, {{tube_name = tube_name}})
        end)
        if ok and err == nil then
            oks[method] = 1
        else
            errors[method] = 1
        end
    end

    local metrics = get_metrics(tube_name, 'queue-storage-1-0')

    assert_metric(metrics, "tnt_sharded_queue_storage_role_stats_count", "method",
        oks, {status = "ok"})
    assert_metric(metrics, "tnt_sharded_queue_storage_role_stats_count", "method",
        errors, {status = "error"})
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_calls_total", "state", {
        done = 0,
        take = 0,
        kick = 0,
        bury = 1,
        put = 0,
        delete = 0,
        touch = 0,
        ack = 0,
        release = 0,
    })
    assert_metric(metrics, "tnt_sharded_queue_storage_statistics_tasks", "state", {
        ready = 0,
        taken = 0,
        done = 0,
        buried = 0,
        delayed = 0,
        total = 0,
    })
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats"), {})
    t.assert_not_equals(get_metric(metrics, "tnt_sharded_queue_storage_role_stats_sum"), {})
end
