#!/usr/bin/env tarantool

local t = require('luatest')
local g = t.group('api')

local _, api = pcall(require, 'sharded_queue.api')
local helper = require('test.helper')
local utils = require('test.helper.utils')
local tube = require('sharded_queue.router.tube')
local is_metrics_supported = utils.is_metrics_supported()

g.before_all(function()
    t.skip_if(utils.is_tarantool_3(), 'the role is available only for Cartridge')
    g.queue_conn = helper.get_evaler('queue-router')
    g.queue_conn_ro = helper.get_evaler('queue-router-1')
    g.cfg = helper.get_cfg()
end)

g.after_each(function()
    helper.set_cfg(g.cfg)
end)

g.test_exported_api = function()
    for _, method in pairs(tube.get_methods()) do
        t.assert_type(api[method], 'function',
            string.format('api role has method "%s" exported', method))
    end

    t.assert_type(api.statistics, 'function',
        'api role has method "statistics" exported')
end

g.test_tube_wrapper = function()
    local tube_name = 'test_tube_wrapper'
    g.queue_conn:call('queue.create_tube', { tube_name })

    local cmd = ("return require('sharded_queue.api').put('%s', 'task_1')")
        :format(tube_name)
    local result = g.queue_conn:eval(cmd)
    t.assert_equals(result[utils.index.data], 'task_1')

    cmd = ("return require('sharded_queue.api').take('%s')"):format(tube_name)
    result = g.queue_conn:eval(cmd)
    t.assert_equals(result[utils.index.data], 'task_1')

    cmd = ("return require('sharded_queue.api').ack('%s', %s)")
        :format(tube_name, result[utils.index.task_id])
    result = g.queue_conn:eval(cmd)
    t.assert_equals(result[utils.index.data], 'task_1')
end

g.test_role_statistics = function()
    local tube_name = 'test_tube_wrapper'
    g.queue_conn:call('queue.create_tube', { tube_name })

    local cmd = ("return require('sharded_queue.api').statistics('%s')")
        :format(tube_name)
    local result = g.queue_conn:eval(cmd)
    t.assert_type(result, 'table')
end

g.test_create_cfg = function()
    t.assert_error_msg_contains('a tube name "cfg" is reserved', function()
        g.queue_conn:call('queue.create_tube', { 'cfg' })
    end)
end

g.test_role_cfg_default = function()
    local cfg = g.queue_conn:eval("return require('sharded_queue.api').cfg")
    t.assert_equals({metrics = is_metrics_supported}, cfg)
end

g.test_role_cfg_metrics_switch = function()
    t.skip_if(not is_metrics_supported, "metrics >= 0.11.0 is not installed")

    local cfg = g.queue_conn:eval("return require('sharded_queue.api').cfg")
    t.assert_equals({metrics = true}, cfg)

    g.queue_conn:eval("require('sharded_queue.api').cfg(...)", {{metrics = false}})
    cfg = g.queue_conn:eval("return require('sharded_queue.api').cfg")
    t.assert_equals({metrics = false}, cfg)

    g.queue_conn:eval("require('sharded_queue.api').cfg(...)", {{metrics = true}})
    cfg = g.queue_conn:eval("return require('sharded_queue.api').cfg")
    t.assert_equals({metrics = true}, cfg)
end

g.test_role_cfg_assignment = function()
    t.skip_if(not is_metrics_supported, "metrics >= 0.11.0 is not installed")

    t.assert_error_msg_contains('Use api.cfg() instead', function()
        g.queue_conn:eval("require('sharded_queue.api').cfg['metrics'] = false")
    end)
end

g.test_role_statistics_read_only_router = function()
    --make sure queue_conn_ro is read_only
    local ro = g.queue_conn_ro:eval("return box.cfg.read_only")
    t.assert_equals(ro, true)

    --create queue and put task using read_only router connection
    local tube_name = 'test_tube_read_only_router'
    g.queue_conn_ro:call('queue.create_tube', { tube_name })

    local cmd = ("return require('sharded_queue.api').put('%s', 'task_1')"):format(tube_name)
    local result = g.queue_conn_ro:eval(cmd)
    t.assert_equals(result[utils.index.data], 'task_1')

    cmd = ("return require('sharded_queue.api').statistics('%s')"):format(tube_name)
    local result_m = g.queue_conn:eval(cmd)
    local result_ro = g.queue_conn_ro:eval(cmd)

    t.assert_type(result_m, 'table')
    t.assert_type(result_ro, 'table')
    t.assert_equals(result_m, result_ro)
end

g.test_api_version = function()
    local api_conn = helper.get_evaler('queue-router')
    local storage_conn = helper.get_evaler('queue-storage-1-0')

    local api_version = api_conn:eval(
        "return require('sharded_queue.api')._VERSION"
    )
    local storage_version = storage_conn:eval(
        "return require('sharded_queue.storage')._VERSION"
    )

    t.assert_equals(api_version, storage_version)
    t.assert_not_equals(string.find(api_version, "^%d+%.%d+%.%d+$"), nil)
    t.assert_not_equals(string.find(storage_version, "^%d+%.%d+%.%d+$"), nil)
end
