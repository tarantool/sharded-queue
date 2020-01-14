#!/usr/bin/env tarantool

local t = require('luatest')
local g = t.group('tagged_driver')

local config = require('test.helper.config')
local utils = require('test.helper.utils')

local function shape_cmd(tube_name, cmd)
    return string.format('queue.tube.%s:%s', tube_name, cmd)
end

g.before_all = function()
    g.queue_conn = config.cluster:server('queue-router').net_box
    g.tube_name = 'tagged_queue'
end

g.setup = function()
    g.queue_conn:call('queue.create_tube', { g.tube_name, { driver = 'sharded_queue.drivers.tagged' } })
end

g.teardown = function()
    g.queue_conn:call(shape_cmd(g.tube_name, 'drop'))
end

local function put_task(tube_name, data, tags)
    return g.queue_conn:call(shape_cmd(tube_name, 'put'), {data, {tags = tags}})
end

local function take_task(tube_name, tags, timeout)
    return g.queue_conn:call(shape_cmd(tube_name, 'take'), {timeout, {tags = tags}})
end

g.test_take_positive = function()
    local data, timeout = 'task_1', 0.1
    local test_table = {
        {
            task_tags = {},
            take_tags = {}
        },
        {
            task_tags = {'a'},
            take_tags = {'a'}
        },
        {
            task_tags = {'a'},
            take_tags = {'a', 'b'}
        },
        {
            task_tags = {'a', 'b'},
            take_tags = {'a', 'b', 'c'}
        },
        {
            task_tags = {'a', 'a'},
            take_tags = {'a'}
        },
        {
            task_tags = {'a'},
            take_tags = {'a', 'a'}
        }
    }

    for case_num, test_case in ipairs(test_table) do
        local result = put_task(g.tube_name, data, test_case.task_tags)
        local fail_msg = ("case num: %d, task tags: %s, take tags: %s"):format(
            case_num, test_case.task_tags, test_case.take_tags
        )
        t.assert_equals(
            result[utils.index.data], data, fail_msg
        )
        result = take_task(g.tube_name, test_case.take_tags, timeout)
        t.assert_equals(result[utils.index.data], data, fail_msg)
    end
end

g.test_take_negative = function()
    local data, timeout = 'task_1', 0.1
    local test_table = {
        {
            task_tags = {'a'},
            take_tags = {}
        },
        {
            task_tags = {},
            take_tags = {'a'}
        },
        {
            task_tags = {'a'},
            take_tags = {'b'}
        },
        {
            task_tags = {'a', 'b'},
            take_tags = {'c', 'd'}
        },
        {
            task_tags = {'a', 'b', 'c'},
            take_tags = {'a', 'b'}
        }
    }

    for case_num, test_case in ipairs(test_table) do
        local result = put_task(g.tube_name, data, test_case.task_tags)
        local fail_msg = ("case num: %d, task tags: %s, take tags: %s"):format(
            case_num, test_case.task_tags, test_case.take_tags
        )
        t.assert_equals(
            result[utils.index.data], data, fail_msg
        )

        result = take_task(g.tube_name, test_case.take_tags, timeout)
        t.assert_equals(result, nil, fail_msg)

        result = take_task(g.tube_name, test_case.task_tags)
        t.assert_equals(result[utils.index.data], data, fail_msg)
    end
end

g.test_take_with_space_scan = function()
    local data_count = 100
    local tasks, tags = {}, {}
    for i = 1, data_count do
        tasks[i] = 'task_' .. tostring(i)
        tags[i] = {'tag_' .. tostring(i)}
    end

    for i = 1, data_count do
        put_task(g.tube_name, tasks[i], tags[i])
    end

    for i = data_count, 1, -1 do
        local result = take_task(g.tube_name, tags[i])
        t.assert_equals(result[utils.index.data], tasks[i])
    end
end