local cluster = require('cluster')
local net_box = require('net.box')
local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')

local time = require('queue.time')
local state = require('queue.state')
local utils = require('queue.utils')

local tube = require('queue.driver_fifottl')


local remote_call = function(method, instance_uri, args)
    local conn = net_box.connect(instance_uri)
    local ret = conn:call(method, { args })
    conn:close()
    return ret
end

local service = {}

function service.create(tube_name, options)
    local tubes = cluster.config_get_deepcopy('tubes') or {}

    local key = function(x) return x.name end
    if utils.array_contains(tubes, tube_name, key) then
        return false
    end

    table.insert(tubes, { name = tube_name, options = options })

    cluster.config_patch_clusterwide({ tubes = tubes })

    return true
end

function service.put(tube_name, data, options)
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

function service.take(tube_name, timeout)
    -- take task from tube --

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        log.info(replica.master.uri)
        if utils.array_contains(replica.roles, 'queue.storage') then
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

function service.delete(tube_name, task_id)
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

function service.release(tube_name, task_id)
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

function service.touch(tube_name, task_id, delta)
    if delta == nil or delta <= 0 then
        return
    end

    if delta >= time.MAX_TIMEOUT then
        delta = time.TIMEOUT_INFINITY
    else
        delta = time.nano(delta)
    end
    log.info("TOUCH")
    log.info(task_id)
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
        
        rawset(_G, 'create_tube', service.create)
        box.schema.func.create('create_tube')
        box.schema.user.grant('guest', 'execute', 'function', 'create_tube')

        rawset(_G, 'tube_put', service.put)
        box.schema.func.create('tube_put')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_put')

        rawset(_G, 'tube_take', service.take)
        box.schema.func.create('tube_take')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_take')

        rawset(_G, 'tube_release', service.release)
        box.schema.func.create('tube_release')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_release')

        rawset(_G, 'tube_delete', service.delete)
        box.schema.func.create('tube_delete')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_delete')

        rawset(_G, 'tube_touch', service.touch)
        box.schema.func.create('tube_touch')
        box.schema.user.grant('guest', 'execute', 'function', 'tube_touch')
    end
end

return {
    init = init,
    service = service,
    validate_config = validate_config,
    apply_config = apply_config,

    dependencies = {
        'cluster.roles.vshard-router',
    }
}
