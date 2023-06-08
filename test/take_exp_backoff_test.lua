local t = require('luatest')
local g = t.group('exponential_backoff_test')

local log = require('log') -- luacheck: ignore

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')

g.before_all(function()
    --- Workaround for https://github.com/tarantool/cartridge/issues/462
    config.cluster:server('queue-router').net_box:close()
    config.cluster:server('queue-router').net_box = nil
    config.cluster:server('queue-router'):connect_net_box()
    g.queue_conn = config.cluster:server('queue-router').net_box
end)

local function task_take(tube_name, timeout, channel, options)
    -- fiber function for take task with timeout and finish time
    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { timeout, options })

    channel:put(fiber.time64())
    channel:put(task)
end

function g.test_default_wait_factor()
    local tube_name = 'test_default_wait_factor'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local timeout = 10 -- second
    local attemts = 8 -- attempts count
    local put_wait = 0.01*(math.pow(2, attemts) - 1) -- 2.55
    local take_time_expected = 0.01*(math.pow(2, attemts+1) - 1) -- 5.12

    local channel = fiber.channel(2)
	local start = fiber.time64()
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(put_wait + 1)

    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=5}))

    local waiting_time = tonumber(channel:get() - start) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, take_time_expected, 0.75)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end

function g.test_success()
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.05
    -- 0.06 : take fail => wait 0.25
    -- 0.31 : take fail => wait 1.25
    -- 1    : put task
    -- 1.56 : take success
    -- expected time is 1.56 in case wait_factor = 5

    local tube_name = 'test_success_exp_backoff'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 5,
        }
    })

    local timeout = 10

    local channel = fiber.channel(2)
	local start = fiber.time64()
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(1)
    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=5}))

    local waiting_time = tonumber(channel:get() - start) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 1.56, 0.75)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end

function g.test_invalid_factors()

    local tube_name = 'test_tinvalid_factors'

    t.assert_error_msg_contains('wait_factor', g.queue_conn.call,
        g.queue_conn,
        'queue.create_tube',
        { tube_name, {
            wait_factor = 0.5,
        }
    })

    t.assert_error_msg_contains('wait_factor', g.queue_conn.call,
        g.queue_conn,
        'queue.create_tube',
        { tube_name, {
            wait_factor = 'not factor',
        }
    })
end

function g.test_invalid_wait_max()

    local tube_name = 'test_invalid_wait_max'

    t.assert_error_msg_contains('wait_max', g.queue_conn.call,
        g.queue_conn,
        'queue.create_tube',
        { tube_name, {
            wait_max = -8,
        }
    })

    t.assert_error_msg_contains('wait_max', g.queue_conn.call,
        g.queue_conn,
        'queue.create_tube',
        { tube_name, {
            wait_max = 0,
        }
    })

    t.assert_error_msg_contains('wait_max', g.queue_conn.call,
        g.queue_conn,
        'queue.create_tube',
        { tube_name, {
            wait_max = 'not number',
        }
    })
end

function g.test_invalid_wait_max_on_take()
    -- wait_max = 1
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.05
    -- 0.06 : take fail => wait 0.25
    -- 0.31 : take fail => wait min(1.25, 1) = 1
    -- 1.31 : take fail => wait 1
    -- 2.31 : take fail => wait 1
    -- 2.8  : put task
    -- 3.31 : take success
    -- expected time is 3.31 in case wait_factor = 5

    local tube_name = 'test_invalid_wait_max_on_take'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 5,
            wait_max = 1,
        }
    })

    local timeout = 10

    local channel = fiber.channel(2)
	local start = fiber.time64()
    fiber.create(task_take, tube_name, timeout, channel, {
        wait_max = -10, -- invalid wait_factor, using 0.3
    })

    fiber.sleep(2.8)
    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=5}))

    local waiting_time = tonumber(channel:get() - start) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 3.31, 0.75)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end

function g.test_wait_max_on_tube()
    -- wait_max = 1
    -- wait_factor = 2 (default value)
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.02
    -- 0.03 : take fail => wait 0.04
    -- 0.07 : take fail => wait 0.08
    -- 0.15 : take fail => wait 0.16
    -- 0.31 : take fail => wait 0.32
    -- 0.63 : take fail => wait 0.64
    -- 1.27 : take fail => wait 1
    -- 2.27 : take fail => wait 1
    -- ~2.8 : put task with ttl = 1
    -- 3.27 : take successful
    -- expected time is 3.27

    local tube_name = 'test_wait_max_on_take_tube'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_max = 1,
        }
    })

    local timeout = 10
    local channel = fiber.channel(2)
	local start = fiber.time64()
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(2.8)
    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'),
        { 'simple_task' }, {timeout=5}))

    local waiting_time = tonumber(channel:get() - start) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 3.27, 0.75)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end

function g.test_wait_max_in_take()
    -- wait_max = 1
    -- wait_factor = 2 (default value)
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.02
    -- 0.03 : take fail => wait 0.04
    -- 0.07 : take fail => wait 0.08
    -- 0.15 : take fail => wait 0.16
    -- 0.31 : take fail => wait 0.32
    -- 0.63 : take fail => wait 0.64
    -- 1.27 : take fail => wait 1
    -- 2.27 : take fail => wait 1
    -- ~2.8 : put task with ttl = 1
    -- 3.27 : take successful
    -- expected time is 3.27

    local tube_name = 'test_wait_max_in_take_tube'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 2,
            wait_max = 100,
        }
    })

    local timeout = 10
    local channel = fiber.channel(2)
	local start = fiber.time64()
    fiber.create(task_take, tube_name, timeout, channel, {wait_max = 1.0})

    fiber.sleep(2.8)
    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'),
        { 'simple_task' }, {timeout=5}))

    local waiting_time = tonumber(channel:get() - start) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 3.27, 0.75)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end
