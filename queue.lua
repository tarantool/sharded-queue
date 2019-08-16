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
    local task = queue._conn:call('tube_put',
        {
            self.tube_name,
            data,
            options
        })
    return task
end

function method.take(self, timeout)
    local task = queue._conn:call('tube_take',
        {
            self.tube_name,
            timeout
        })

    return task
end

function method.delete(self, task_id)
    -- local options = options or {}
    local task = queue._conn:call('tube_delete',
        {
            self.tube_name,
            task_id
        })

    return task
end

function method.release(self, task_id)
    local task = queue._conn:call('tube_release',
        {
            self.tube_name,
            task_id
        })
    return task
end

function method.touch(self, task_id, delay)
    local task = queue._conn:call('tube_touch',
        {
            self.tube_name,
            task_id,
            delay
        })
    return task
end

function method.ask(self, task_id)
    local task = queue._conn:call('tube_ask',
        {
            self.tube_name,
            task_id
        })
    return task
end

function method.bury(self, task_id)
    local task = queue._conn:call('tube_bury',
        {
            self.tube_name,
            task_id
        })
    return task
end

function method.kick(self, count)
    local k = queue._conn:call('tube_kick',
        {
            self.tube_name,
            count
        })
    return k
end

local function statistics(tube_name)
    return queue._conn:call('tube_statistic', { tube_name })
end

local function create_tube(tube_name, options)
    local options = options or {}

    local ok = queue._conn:call('create_tube',
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

local function use_exist_tube(tube_name)
    local self = setmetatable({
        tube_name = tube_name,
    }, {
        __index = method
    })
    return self
end

function queue.init(router_uri)
    queue._conn = netbox.connect(router_uri)
    
    if not queue._conn:is_connected() then
        return false
    end
    
    queue.create_tube = create_tube
    queue.statistics = statistics
    queue.__use_exist_tube = use_exist_tube
    
    function queue.stop()
        queue._conn:close()
    end

    return true
    
end

return queue