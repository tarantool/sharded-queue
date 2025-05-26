local is_cartridge_package, cartridge = pcall(require, 'cartridge')
local fiber = require('fiber')
local vshard = require('vshard')
local log = require('log')

local time = require('sharded_queue.time')
local utils = require('sharded_queue.utils')

local function log_task(op_name, task)
    local task_id = type(task) == 'table' and task[1] or task
    log.info(string.format([[Router[%d] %s: task %s]], fiber.self():id(), op_name, task_id))
end

local function remote_call(method, replicaset, args, timeout)
    return replicaset:callrw(method, { args }, { timeout = timeout })
end

local function take_task(replicasets, options, take_timeout, call_timeout)
    for _, replicaset in ipairs(replicasets) do
        if take_timeout == 0 then
            break
        end
        local begin = time.cur()

        -- Try to take a task from all instances.
        local ok, ret = pcall(remote_call, 'tube_take',
            replicaset,
            options,
            call_timeout
        )

        if ret ~= nil and ok then
            return ret
        end

        local duration = time.cur() - begin
        take_timeout = take_timeout > duration and take_timeout - duration or 0
    end
end

function put(self, data, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id = math.random(bucket_count)

    options = table.deepcopy(options or {})

    if options.priority == nil and options.pri ~= nil then
        options.priority = options.pri
    end

    options.data = data
    options.tube_name = self.tube_name
    options.bucket_id = bucket_id
    options.bucket_count = bucket_count

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id,
        'write', 'tube_put', { options })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('put', task)
    end

    return task
end

function take(self, timeout, options)
    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local remote_call_timeout = time.MIN_NET_BOX_CALL_TIMEOUT
    if timeout ~= nil and timeout > time.MIN_NET_BOX_CALL_TIMEOUT then
        remote_call_timeout = timeout
    end

    local take_timeout = time.nano(timeout) or time.TIMEOUT_INFINITY

    local frequency = 1000
    local wait_part = 0.01 -- maximum waiting time in second
    local wait_max = utils.normalize.wait_max(options.wait_max)
        or self.wait_max or time.MAX_TIMEOUT

    local wait_factor = self.wait_factor

    local calc_part = time.sec(take_timeout / frequency)

    if calc_part < wait_part then
        wait_part = tonumber(calc_part)
    end

    if options.extra.log_request then
        log.info(("Router[%d] take: start attempts"):format(fiber.self():id()))
    end

    while take_timeout ~= 0 do
        local begin = time.cur()

        local shards, err = vshard.router.routeall()
        if err ~= nil then
            error(err)
        end

        local replicasets = {}
        for _, replicaset in pairs(shards) do
            table.insert(replicasets, replicaset)
        end
        utils.array_shuffle(replicasets)

        local task = take_task(replicasets,
            options, take_timeout, remote_call_timeout)

        if task ~= nil then
            if options.extra.log_request then
                log_task('take', task)
            end
            return task
        end

        if take_timeout < time.nano(wait_part) then
            if options.extra.log_request then
                log.info(("Router[%d] take: next attemt will be after timeout")
                    :format(fiber.self():id()))
            end
            return nil
        end

        fiber.sleep(wait_part)

        wait_part = wait_part * wait_factor
        if wait_part > wait_max then
            wait_part = wait_max
        end

        local duration = time.cur() - begin

        take_timeout = take_timeout > duration and take_timeout - duration or 0
    end

    if options.extra.log_request then
        log.info(("Router[%d] take: timeout"):format(fiber.self():id()))
    end
end

function delete(self, task_id, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_delete', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('delete', task)
    end

    return task
end

function release(self, task_id, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_release', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('release', task)
    end

    return task
end

function touch(self, task_id, delta, options)
    if delta == nil or delta <= 0 then
        return
    end

    if delta >= time.MAX_TIMEOUT then
        delta = time.TIMEOUT_INFINITY
    else
        delta = time.nano(delta)
    end

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id
    options.delta = delta

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_touch', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('touch', task)
    end

    return task
end

function ack(self, task_id, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    if options.extra.log_request then
        log.info(("Router[%d] ack: call id %d, bucket %d")
            :format(fiber.self():id(), task_id, bucket_id))
    end

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_ack', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('ack', task)
    end

    return task
end

function bury(self, task_id, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_bury', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('bury', task)
    end

    return task
end

function kick(self, count, options)
    local kicked_count = 0 -- count kicked task
    local shards, err = vshard.router.routeall()
    if err ~= nil then
        error(err)
    end

    for _, replicaset in pairs(shards) do
        local opts = table.deepcopy(options or {})
        opts.tube_name = self.tube_name
        opts.count = count - kicked_count

        local ok, k = pcall(remote_call, 'tube_kick', replicaset, opts)
        if not ok then
            log.error(k)
            return kicked_count
        end

        kicked_count = kicked_count + k

        if kicked_count == count then
            break
        end
    end

    return kicked_count
end

function peek(self, task_id, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name
    options.task_id = task_id

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_peek', {
        options
    })
    -- Re-raise storage errors.
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('peek', task)
    end

    return task
end

function drop(self)
    local tubes = cartridge.config_get_deepcopy('tubes') or {}
    tubes[self.tube_name] = nil
    cartridge.config_patch_clusterwide({ tubes = tubes })
end

function truncate(self, options)
    options = table.deepcopy(options or {})
    options.tube_name = self.tube_name

    options.extra = {
        log_request = utils.normalize.log_request(options.log_request) or self.log_request,
    }

    local _, err, alias = vshard.router.map_callrw('tube_truncate', {
        options
    })
    -- Re-raise storage errors.
    if err then
        if alias then
            error("Error occurred on replicaset \"" .. alias .. "\": " .. err.message)
        else
            error("Error occurred: " .. err.message)
        end
        return
    end
end

local methods = {
    put = put,
    take = take,
    delete = delete,
    release = release,
    touch = touch,
    ack = ack,
    bury = bury,
    kick = kick,
    peek = peek,
    truncate = truncate,
}

-- The Tarantool 3.0 does not support to update dinamically a configuration, so
-- a user must update the configuration by itself.
if is_cartridge_package then
    methods.drop = drop
end

local function new_metrics_metatable(metrics)
    local mt = {
        __index = {},
    }

    for call, fun in pairs(methods) do
        mt.__index[call] = function(self, ...)
            local before = fiber.clock()
            local ok, ret = pcall(fun, self, ...)
            local latency = fiber.clock() - before

            metrics.observe(latency, self.tube_name, call, ok)

            if not ok then
                error(ret)
            end

            return ret
        end
    end

    return mt
end

local function new(name, metrics, options)
    return setmetatable({
        tube_name = name,
        wait_max = options.wait_max,
        wait_factor = options.wait_factor or time.DEFAULT_WAIT_FACTOR,
        log_request = utils.normalize.log_request(options.log_request),
    }, new_metrics_metatable(metrics))
end

local function get_methods()
    local list = {}
    for method, _ in pairs(methods) do
        table.insert(list, method)
    end
    return list
end

return {
    new = new,
    get_methods = get_methods,
}
