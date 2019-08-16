local cluster = require('cluster')
local net_box = require('net.box')
local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')

local time = require('shared_queue.time')
local state = require('shared_queue.state')
local utils = require('shared_queue.utils')

local tube = require('shared_queue.driver_fifottl')


local remote_call = function(method, instance_uri, args)
    local conn = net_box.connect(instance_uri)
    local ret = conn:call(method, { args })
    conn:close()
    return ret
end

local shared_queue = {}

function shared_queue.create(tube_name, options)
    local tubes = cluster.config_get_deepcopy('tubes') or {}

    local key = function(x) return x.name end
    if utils.array_contains(tubes, tube_name, key) then
        return false
    end

    table.insert(tubes, { name = tube_name, options = options })

    cluster.config_patch_clusterwide({ tubes = tubes })

    return true
end

function shared_queue.put(tube_name, data, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id = math.random(bucket_count)
    
    options.data = data
    options.tube_name = tube_name
    options.bucket_id = bucket_id
    options.bucket_count = bucket_count

    local task = vshard.router.call(bucket_id,
        'write', 'tube_put', { options })
    return task
end

-- function for try get task from instance --   
function take_task(tube_name, storages)
    for _, instance_uri in pairs(storages) do
        -- try take task from all instance
        local ok, ret = pcall(remote_call, 'tube_take',
            instance_uri,
            {
                tube_name = tube_name
            })
        if ret ~= nil then
            return ret
        end
    end
end

function shared_queue.take(tube_name, timeout)
    -- take task from tube --

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        log.info(replica.master.uri)
        if utils.array_contains(replica.roles, 'shared_queue.storage') then
            table.insert(storages, replica.master.uri)
        end
    end 
    utils.array_shuffle(storages)

    local task = take_task(tube_name, storages)
    
    if task ~= nil then
        return task
    end

    timeout = time.nano(timeout) or time.TIMEOUT_INFINITY

    local frequency = 1000
    local wait_part = 0.01 -- maximum waiting time in second

    local calc_part = time.sec(timeout / frequency) 
    
    if calc_part < wait_part then
        wait_part = tonumber(calc_part)
    end

    while timeout ~= 0 do
        local begin = time.cur()

        local cond = fiber.cond()
        cond:wait(wait_part)

        task = take_task(tube_name, storages)

        if task ~= nil then
            return task
        end

        local duration = time.cur() - begin
        timeout = timeout > duration and timeout - duration or 0
    end
end

function shared_queue.delete(tube_name, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_delete', {
        {
            tube_name = tube_name,
            task_id = task_id    
        }
    })

    return task
end

function shared_queue.release(tube_name, task_id)
    -- task release from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_release', {
        {
            tube_name = tube_name,
            task_id = task_id    
        }
    })

    return task
end

function shared_queue.touch(tube_name, task_id, delta)
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

    local task = vshard.router.call(bucket_id, 'write', 'tube_touch', {
        {
            tube_name = tube_name,
            task_id = task_id,
            delta = delta    
        }
    })
    log.info(task)
    return task
end

function shared_queue.ask(tube_name, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_ask', {
        {
            tube_name = tube_name,
            task_id = task_id    
        }
    })

    return task
end

function shared_queue.bury(tube_name, task_id)
    -- task bury --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_bury', {
        {
            tube_name = tube_name,
            task_id = task_id    
        }
    })

    return task
end

function shared_queue.kick(tube_name, count)
    -- try kick few tasks --

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        if utils.array_contains(replica.roles, 'shared_queue.storage') then
            table.insert(storages, replica.master.uri)
        end
    end

    local kicked_count = 0 -- count kicked task
    for _, instance_uri in pairs(storages) do
        local ok, k = pcall(remote_call, 'tube_kick',
            instance_uri,
            {
                tube_name = tube_name,
                count     = count - kicked_count
            })
        if not ok then
            log.error(k)
            return
        end
        
        kicked_count = kicked_count + k
        
        if kicked_count == count then
            break
        end
    end

    return kicked_count
end

function shared_queue.statistic(tube_name)
    log.info('statistic')
    log.info(tube_name)
    if not tube_name then
        return
    end
    -- collect stats from all storages
    local stats_collection = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        if utils.array_contains(replica.roles, 'shared_queue.storage') then
            local ok, ret = pcall(remote_call, 'tube_statistic',
                replica.master.uri,
                {
                    tube_name = tube_name
                })
            log.info(ret)
            if not ok then
                return
            end
            table.insert(stats_collection, ret)
        end
    end
    -- merge
    local stat = stats_collection[1]
    for i = 2, #stats_collection do
        for name, count in pairs(stats_collection[i].tasks) do
            stat.tasks[name] = stat.tasks[name] + count
        end
        for name, count in pairs(stats_collection[i].calls) do
            stat.calls[name] = stat.calls[name] + count
        end
    end
    --
    return stat
end

local function validate_config(config_new, config_old)
    return true
end

local function apply_config(cfg, opts)
    return true
end

local function init(opts)
    

    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
        
        rawset(_G, 'create_tube', shared_queue.create)
        box.schema.func.create('create_tube')
        box.schema.user.grant('guest', 'execute', 'function', 'create_tube')

        rawset(_G, 'tube_put', shared_queue.put)
        box.schema.func.create('tube_put')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_put')

        rawset(_G, 'tube_take', shared_queue.take)
        box.schema.func.create('tube_take')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_take')

        rawset(_G, 'tube_release', shared_queue.release)
        box.schema.func.create('tube_release')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_release')

        rawset(_G, 'tube_delete', shared_queue.delete)
        box.schema.func.create('tube_delete')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_delete')

        rawset(_G, 'tube_touch', shared_queue.touch)
        box.schema.func.create('tube_touch')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_touch')

        rawset(_G, 'tube_ask', shared_queue.ask)
        box.schema.func.create('tube_ask')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_ask')

        rawset(_G, 'tube_bury', shared_queue.bury)
        box.schema.func.create('tube_bury')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_bury')

        rawset(_G, 'tube_kick', shared_queue.kick)
        box.schema.func.create('tube_kick')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_kick')

        rawset(_G, 'tube_statistic', shared_queue.statistic)
        box.schema.func.create('tube_statistic')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_statistic')
    end
end

return {
    init = init,
    shared_queue = shared_queue,
    validate_config = validate_config,
    apply_config = apply_config,

    dependencies = {
        'cluster.roles.vshard-router',
    }
}
