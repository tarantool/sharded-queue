local cartridge = require('cartridge')
local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')

local metrics = require('sharded_queue.metrics')
local stash = require('sharded_queue.stash')
local state = require('sharded_queue.state')
local time = require('sharded_queue.time')
local utils = require('sharded_queue.utils')

local stash_names = {
    cfg = '__sharded_queue_api_cfg',
    metrics_stats = '__sharded_queue_api_metrics_stats',
}
stash.setup(stash_names)

local remote_call = function(method, replicaset, args, timeout)
    return replicaset:callrw(method, { args }, { timeout = timeout })
end

local function validate_options(options)
    if not options then return true end

    if options.wait_factor then
        if type(options.wait_factor) ~= 'number'
            or options.wait_factor < 1
        then
            return false, "wait_factor must be number greater than or equal to 1"
        end
    end

    local _, err = utils.normalize.log_request(options.log_request)
    if err then
        return false, err
    end

    if options.wait_max ~= nil then
        local err
        options.wait_max, err = utils.normalize.wait_max(options.wait_max)
        if err ~= nil then
            return false, err
        end
    end

    return true
end

local function log_task(op_name, task)
    local task_id = type(task) == 'table' and task[1] or task
    log.info(string.format([[Router[%d] %s: task %s]], fiber.self():id(), op_name, task_id))
end

local sharded_tube = {}

function sharded_tube.put(self, data, options)
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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('put', task)
    end

    return task
end

-- function for try get task from instance --
local function take_task(replicasets, options, take_timeout, call_timeout)
    for _, replicaset in ipairs(replicasets) do
        if take_timeout == 0 then
            break
        end
        local begin = time.cur()

        -- try take task from all instance
        local ok, ret = pcall(remote_call, 'tube_take',
            replicaset,
            options,
            call_timeout
        )

        if ret ~= nil and ok then
            return ret
        end

        local duration = time.cur() - begin
        take_timeout = take_timeout > duration and take_timeout -  duration or 0
    end
end

function sharded_tube.take(self, timeout, options)
    -- take task from tube --
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

function sharded_tube.delete(self, task_id, options)
    -- task delete from tube --

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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('delete', task)
    end

    return task
end

function sharded_tube.release(self, task_id, options)
    -- task release from tube --

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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('release', task)
    end

    return task
end

function sharded_tube.touch(self, task_id, delta, options)
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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('touch', task)
    end

    return task
end

function sharded_tube.ack(self, task_id, options)
    -- task delete from tube --

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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('ack', task)
    end

    return task
end

function sharded_tube.bury(self, task_id, options)
    -- task bury --

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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('bury', task)
    end

    return task
end

function sharded_tube.kick(self, count, options)
    -- try kick few tasks --

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

function sharded_tube.peek(self, task_id, options)
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
    -- re-raise storage errors
    if err ~= nil then error(err) end

    if options.extra.log_request then
        log_task('peek', task)
    end

    return task
end

function sharded_tube.drop(self)
    local tubes = cartridge.config_get_deepcopy('tubes') or {}

    tubes[self.tube_name] = nil
    cartridge.config_patch_clusterwide({ tubes = tubes })
end

local sharded_queue = {
    tube = {},
    cfg = stash.get(stash_names.cfg),
    metrics_stats = metrics.init(stash.get(stash_names.metrics_stats)),
}

if sharded_queue.cfg.metrics == nil then
    sharded_queue.cfg.metrics = true
end

if sharded_queue.cfg.metrics then
    sharded_queue.cfg.metrics = metrics.is_supported()
end

local function wrap_sharded_queue_call_counters(call, fun)
    return function(self, ...)
        local before = fiber.clock()
        local ok, ret = pcall(fun, self, ...)
        local after = fiber.clock()

        if sharded_queue.cfg.metrics then
            sharded_queue.metrics_stats:observe(after - before,
                self.tube_name, call, ok)
        end

        if not ok then
            error(ret)
        end

        return ret
    end
end

for call, fun in pairs(sharded_tube) do
    sharded_tube[call] = wrap_sharded_queue_call_counters(call, fun)
end

local function metrics_enable()
    local get_statistic = function(tube)
        return sharded_queue.statistics(tube)
    end
    sharded_queue.metrics_stats:enable('api', sharded_queue.tube, get_statistic)
end

local function metrics_disable()
    sharded_queue.metrics_stats:disable()
end

function sharded_queue.cfg_call(_, options)
    options = options or {}
    if options.metrics == nil then
        return
    end

    if type(options.metrics) ~= 'boolean' then
        error('"metrics" must be a boolean')
    end

    if sharded_queue.cfg.metrics ~= options.metrics then
        local tubes = cartridge.config_get_deepcopy('tubes') or {}

        if tubes['cfg'] ~= nil and tubes['cfg'].metrics == nil then
            error('tube "cfg" exist, unable to update a default configuration')
        end

        tubes['cfg'] = {metrics = options.metrics}
        local ok, err = cartridge.config_patch_clusterwide({ tubes = tubes })
        if not ok then error(err) end
    end
end

function sharded_queue.statistics(tube_name)
    if not tube_name then
        return
    end

    local stats_collection, err = vshard.router.map_callrw('tube_statistic',
        {{ tube_name = tube_name }})
    if err ~= nil then
        return nil, err
    end

    if type(stats_collection) ~= 'table' then
        return nil, 'No stats retrieved'
    end

    if next(stats_collection) == nil then
        return nil
    end

    local stat = { tasks = {}, calls = {} }
    for _, replicaset_stats in pairs(stats_collection) do
        if type(replicaset_stats) ~= 'table' or next(replicaset_stats) == nil then
            return nil, 'Invalid stats'
        end

        for name, count in pairs(replicaset_stats[1].tasks) do
            stat.tasks[name] = (stat.tasks[name] or 0) + count
        end
        for name, count in pairs(replicaset_stats[1].calls) do
            stat.calls[name] = (stat.calls[name] or 0) + count
        end
    end

    return stat
end

function sharded_queue.create_tube(tube_name, options)
    local tubes = cartridge.config_get_deepcopy('tubes') or {}

    if tube_name == 'cfg' then
        error('a tube name "cfg" is reserved')
    end

    if tubes[tube_name] ~= nil then
        -- already exist --
        return nil
    end

    local ok , err = validate_options(options)
    if not ok then error(err) end

    options = table.deepcopy(options or {})
    if options.priority == nil and options.pri ~= nil then
        options.priority = options.pri
    end

    tubes[tube_name] = options
    ok, err = cartridge.config_patch_clusterwide({ tubes = tubes })
    if not ok then error(err) end

    return sharded_queue.tube[tube_name]
end

local function init(opts)
    rawset(_G, 'queue', sharded_queue)
end

local function validate_config(cfg)
    local cfg_tubes = cfg.tubes or {}
    local ok, err = utils.validate_tubes(cfg_tubes, false)
    if not ok then
        return ok, err
    end
    return utils.validate_cfg(cfg_tubes['cfg'])
end

local function apply_config(cfg, opts)
    local cfg_tubes = cfg.tubes or {}
    -- try init tubes --
    for tube_name, options in pairs(cfg_tubes) do
        if tube_name == 'cfg' then
            if options.metrics ~= nil then
                sharded_queue.cfg.metrics = options.metrics and true or false
            end
        elseif sharded_queue.tube[tube_name] == nil then
            local self = setmetatable({
                tube_name = tube_name,
                wait_max = options.wait_max,
                wait_factor = options.wait_factor or time.DEFAULT_WAIT_FACTOR,
                log_request = utils.normalize.log_request(options.log_request),
            }, {
                __index = sharded_tube,
            })
            sharded_queue.tube[tube_name] = self
        end
    end

    -- try drop tubes --
    for tube_name, _ in pairs(sharded_queue.tube) do
        if tube_name ~= 'cfg' and cfg_tubes[tube_name] == nil then
            setmetatable(sharded_queue.tube[tube_name], nil)
            sharded_queue.tube[tube_name] = nil
        end
    end

    if sharded_queue.cfg.metrics then
        metrics_enable()
    else
        metrics_disable()
    end
end

-- FIXME: Remove when https://github.com/tarantool/cartridge/issues/308 resolved
local function queue_action_wrapper(action)
    return function(name, ...)
        if not sharded_queue.tube[name] then
            return nil, string.format('No queue "%s" initialized yet', name)
        end
        return sharded_queue.tube[name][action](sharded_queue.tube[name], ...)
    end
end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,

    put = queue_action_wrapper('put'),
    take = queue_action_wrapper('take'),
    delete = queue_action_wrapper('delete'),
    release = queue_action_wrapper('release'),
    touch = queue_action_wrapper('touch'),
    ack = queue_action_wrapper('ack'),
    bury = queue_action_wrapper('bury'),
    kick = queue_action_wrapper('kick'),
    peek = queue_action_wrapper('peek'),
    drop = queue_action_wrapper('drop'),

    cfg = setmetatable({}, {
        __index = sharded_queue.cfg,
        __newindex = function() error("Use api.cfg() instead", 2) end,
        __call = sharded_queue.cfg_call,
        __serialize = function() return sharded_queue.cfg end,
    }),
    statistics = sharded_queue.statistics,
    _VERSION = require('sharded_queue.version'),

    dependencies = {
        'cartridge.roles.vshard-router',
    },

    __private = {
        sharded_tube = sharded_tube,
    }
}
