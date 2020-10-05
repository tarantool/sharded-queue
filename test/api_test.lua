#!/usr/bin/env tarantool

local t = require('luatest')
local g = t.group('api')

local api = require('sharded_queue.api')
local config = require('test.helper.config')
local utils = require('test.helper.utils')

g.before_all(function()
    g.queue_conn = config.cluster:server('queue-router').net_box
    g.queue_conn_1 = config.cluster:server('queue-router-1').net_box
end)

g.test_exported_api = function()
    for method, _ in pairs(api.__private.sharded_tube) do
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

g.test_role_statistics_read_only_router = function()
    --create queue and put task using read_only router connection
    local tube_name = 'test_tube_read_only_router'
    g.queue_conn_1:call('queue.create_tube', { tube_name })

    local cmd = ("return require('sharded_queue.api').put('%s', 'task_1')"):format(tube_name)
    local result = g.queue_conn_1:eval(cmd)
    t.assert_equals(result[utils.index.data], 'task_1')

    cmd = ("return require('sharded_queue.api').statistics('%s')"):format(tube_name)
    local result_m = g.queue_conn:eval(cmd)
    local result_ro = g.queue_conn_1:eval(cmd)

    t.assert_type(result_m, 'table')
    t.assert_type(result_ro, 'table')
    t.assert_equals(result_m, result_ro)
end