local t = require('luatest')
local g = t.group('metrics_test')

local config = require('test.helper.config')
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

local function filter_metrics(metrics, label, value)
    local filtered = {}
    for _, v in pairs(metrics) do
        if v.label_pairs and v.label_pairs[label] and v.label_pairs[label] == value then
            table.insert(filtered, v)
        end
    end
    return filtered
end

local function get_metrics(tube_name)
    local metrics = g.queue_conn:eval([[
local metrics = require('metrics')
metrics.invoke_callbacks()
return metrics.collect()
]])
    for _, v in ipairs(metrics) do
        v.timestamp = nil
    end

    return filter_metrics(metrics, "name", tube_name)
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

local function assert_metric(metrics, name, label, values)
    local metric = get_metric(metrics, name)
    for k, v in pairs(values) do
        local filtered = filter_metrics(metric, label, k)
        t.assert_equals(#filtered, 1, label .. "_" .. k)
        t.assert_equals(filtered[1].value, v, label .. "_" .. k)
    end
end

g.test_metrics = function()
    local tube_name = 'metrics_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local task_count = 64
    for i = 1, task_count do
        g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
            i, { delay = 3 , ttl = 3, ttr = 1}
        })
    end
    -- check all putten task
    local metrics = get_metrics(tube_name)
    assert_metric(metrics, "sharded_queue_calls", "status", {
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
    assert_metric(metrics, "sharded_queue_tasks", "status", {
        ready = 0,
        taken = 0,
        done = 0,
        buried = 0,
        delayed = task_count,
        total = task_count,
    })
end

g.test_metrics_disabled = function()
    local tube_name = 'metrics_disabled_test'
    g.queue_conn:eval("require('sharded_queue.api').cfg(...)",
        {{metrics = false}})

    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        1, { delay = 3 , ttl = 3, ttr = 1}
    })
    local metrics = get_metrics(tube_name)
    t.assert_equals(metrics, {})
end

g.test_metrics_disable = function()
    local tube_name = 'metrics_disable_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        1, { delay = 3 , ttl = 3, ttr = 1}
    })
    local metrics = get_metrics(tube_name)
    assert_metric(metrics, "sharded_queue_calls", "status", {
        put = 1,
    })
    assert_metric(metrics, "sharded_queue_tasks", "status", {
        delayed = 1,
    })

    g.queue_conn:eval("require('sharded_queue.api').cfg(...)",
        {{metrics = false}})

    metrics = get_metrics(tube_name)
    t.assert_equals(metrics, {})
end
