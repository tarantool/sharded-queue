local cluster = require('cluster')
local net_box = require('net.box')
local vshard = require('vshard')
local state = require('state')
local fiber = require('fiber')
local json = require('json')
local uuid = require('uuid')
local utils = require('utils')
local tube = require('tube_fifo')

local MAX_TIMEOUT      = 365 * 86400 * 100       -- MAX_TIMEOUT == 100 years
local TIMEOUT_INFINITY = 18446744073709551615ULL -- Set to TIMEOUT_INFINITY

local function time(tm)
    if tm == nil then
        tm = fiber.time64()
    elseif tm < 0 then
        tm = 0
    else
        tm = tm * 1000000
    end
    return 0ULL + tm
end

local function event_time(tm)
    if tm == nil or tm < 0 then
        tm = 0
    elseif tm > MAX_TIMEOUT then
        return TIMEOUT_INFINITY
    end
    tm = 0ULL + tm * 1000000 + fiber.time64()
    return tm
end

function map_task_id(tube_name)
    local replicasets = cluster.admin.get_replicasets()
    local id_set = {}
    local args = {tube_name = tube_name}

    local call_get_task_id = function(instance_uri, args)
        local conn = net_box.connect(instance_uri)
        local ret = conn:call('tube_get_task_id', { args })
        conn:close()
        return ret
    end

    for _, replica in pairs(replicasets) do
        if utils.array_contains(replica.roles, 'storage') then
            -- execute only for storage master
            local ok, ret = pcall(call_get_task_id, replica.master.uri, args)
            if ok and ret ~= nil then
                table.insert(id_set, ret)
            end
        end
    end
    return id_set
end

function reduce_task_id(id_set)
    local index, id = 0

    if #id_set ~= 0 then
        index, id = utils.array_max(id_set)
    end

    return id
end

function exec_method_on_instance(instance_uri, method, args)
    local args = args or {}
    local conn = net_box.connect(instance_uri)
    local task = method(conn, args)
    conn:close()
    return task
end

local function map_task(tube_name)
    -- this is test func for collect 'head' 
    -- from all instance (master) and get taken task
    tube_name = tube_name or 'default'

    local replicasets = cluster.admin.get_replicasets()
    local task_set = {}
    for _, replica in pairs(replicasets) do
        if utils.array_contains(replica.roles, 'storage') then
            -- execute only for storage master
            local ok, ret = pcall(exec_method_on_instance, replica.master.uri, tube.remote_get_task)
            if ok and ret ~= nil then
                -- temporary solution
                local _task = {}
                for _, e in pairs(ret:tomap()) do
                    table.insert(_task, tostring(e))
                end
                --
                table.insert(task_set, table.concat(_task, ', '))
            end
        end
    end
    return table.concat(task_set, '\n')
end


-- API HANDLER --
-----------------

local remote_call = function(instance_uri, method, args)
    local conn = net_box.connect(instance_uri)
    local ret = conn:call(method, { args })
    conn:close()
    return ret
end


local handler = {}

function handler.tube_create(req)
    local status, body = pcall(req.json, req)
    local tube_name = req:stash('tube_name')

    local options = body['options']

    local replicasets = cluster.admin.get_replicasets()
    local output = {}
    for _, replica in pairs(replicasets) do
        if utils.array_contains(replica.roles, 'storage') then
            -- execute only for storage master
            local ok, ret = pcall(remote_call,
                replica.master.uri, 'tube_create', { name = tube_name, options = options })
            table.insert(output, tostring(ret))
        end
    end
    return req:render({ json = table.concat(output, '; ')})
end


function handler.tube_put(req)
    local status, body = pcall(req.json, req)
    local tube_name = req:stash('tube_name')

    -- TODO: move parsing request to external function
    local data, priority, ttl, delay = body['data'], body['priority'], body['ttl'], body['delay']

    local task_uuid = uuid.str()
    local bucket_id = vshard.router.bucket_id(task_uuid)

    local replica, err = vshard.router.route(bucket_id)
    local args = {
        tube_name = tube_name,
        task_uuid = task_uuid,
        bucket_id = bucket_id,
        priority = priority,
        ttl = ttl,
        data = data,
        delay = delay
    }

    local ok, ret = pcall(remote_call,
        replica.master.uri, 'tube_put', args)

    return req:render({ json = ret })
end

function handler.tube_take(req)
    local status, body = pcall(req.json, req)
    local tube_name = req:stash('tube_name')
    local timeout = time(body['timeout'] or TIMEOUT_INFINITY)

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        if utils.array_contains(replica.roles, 'storage') then
            table.insert(storages, replica.master.uri)
        end
    end
    
    utils.array_shuffle(storages)

    local take_task = function ()
        for _, instance_uri in pairs(storages) do
            -- try take task from all instance
            local ok, ret = pcall(remote_call,
                instance_uri, 'tube_take', { tube_name = tube_name })
            if ret ~= nil then return ret end
        end
    end
    local task = take_task()
    
    if not task then
        task = {1, 'tube empty'}
    end

    return req:render({ json = task })
end

function handler.tube_delete(req)
    local status, body = pcall(req.json, req)

    local tube_name = req:stash('tube_name')
    local task_uuid = body['task_uuid']

    local bucket_id = vshard.router.bucket_id(task_uuid)
    local replica, err = vshard.router.route(bucket_id)

    local args = {
        tube_name = tube_name,
        task_uuid = task_uuid
    }

    local ok, ret = pcall(remote_call,
        replica.master.uri, 'tube_delete', args)
    if ret == nil then
        ret = {1, 'not found'}
    end

    return req:render({ json = ret })
end

function handler.tube_release(req)
    local status, body = pcall(req.json, req)

    local tube_name = req:stash('tube_name')
    local task_uuid = body['task_uuid']

    local bucket_id = vshard.router.bucket_id(task_uuid)
    local replica, err = vshard.router.route(bucket_id)

    local args = {
        tube_name = tube_name,
        task_uuid = task_uuid
    }

    local ok, ret = pcall(remote_call,
        replica.master.uri, 'tube_release', args)
    if ret == nil then
        ret = {1, 'not found'}
    end

    return req:render({ json = ret })
end

local function init(opts)
    rawset(_G, 'vshard', vshard)

    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
    end

    local httpd = cluster.service_get('httpd')
    if httpd ~= nil then
        httpd:route( { method = 'POST',   path = '/queue/:tube_name/create'  }, handler.tube_create )
        httpd:route( { method = 'PUT',    path = '/queue/:tube_name/put'     }, handler.tube_put )
        httpd:route( { method = 'GET',    path = '/queue/:tube_name/take'    }, handler.tube_take )
        httpd:route( { method = 'DELETE', path = '/queue/:tube_name/delete'  }, handler.tube_delete )
        httpd:route( { method = 'PATCH',  path = '/queue/:tube_name/release' }, handler.tube_release )
    end
end

return {
    init = init,
    dependencies = {
        'cluster.roles.vshard-router',
    }
}
