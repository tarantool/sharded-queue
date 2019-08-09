local netbox = require('net.box')
local fiber = require('fiber')
local time = require('queue.time')

local queue = {}

local function exec_string(conn, str)
    return conn:eval(str)
end

local function remote_call(conn, method, args)
    local ret = conn:call(method, { args })
    return ret
end

local method = {}

function method.put(self, data, options)
    local options = options or {}
    local task = queue._conn:call('queue.put',
        {
            self.tube_name,
            data,
            options
        })
    return task
end

function method.take(self, timeout)
    local timeout = time.time(timeout or time.TIMEOUT_INFINITY)
    local task = queue._conn:call('queue.take',
        {
            self.tube_name,
            timeout
        })
    
    -- if task ~= nil then
    --     return task
    -- end

    -- while timeout > 0 do
    --     local started = fiber.time64()
    --     local t = time.event(timeout)
        
    -- end

    return task
end

function method.delete(self, task_id)
    local options = options or {}
    local task = queue._conn:call('queue.delete',
        {
            self.tube_name,
            task_id
        })
    return task
end

local function apply_config(tube_name, options)
    queue._conn:eval(
        string.format("require('cluster').config_patch_clusterwide({ tube_name = %q, options = {} }) ", tube_name))
end

local function create_tube(tube_name, options)
    local options = options or {}

    local ok = queue._conn:call('queue._create',
        {
            tube_name,
            options
        })
    if ok then
        local self = setmetatable({
            tube_name = tube_name,
        }, {
            __index = method
        })
        return self
    else
        return false
    end
end


function queue.init(router_uri)
    queue._conn = netbox.connect(router_uri)
    
    if not queue._conn:is_connected() then
        return false
    end
    if queue._conn:eval("queue = require('cluster').service_get('queue.api').service") then
        return false
    end

    queue.create_tube = create_tube
    queue.apply_config = apply_config

    function queue.stop()
        queue._conn:close()
    end

    return true
    
end

return queue