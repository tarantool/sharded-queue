local drivers = require('sharded_queue.storage.drivers')

local tubes = {}

local function map_tubes(cfg_tubes)
    cfg_tubes = cfg_tubes or {}

    local result = {}
    for tube_name, tube_opts in pairs(cfg_tubes) do
        if tube_opts.enable == nil or tube_opts.enable == true then
            result[tube_name] = drivers.get(tube_opts.driver)
        end
    end
    return result
end

local function call(self, tube, method, args)
    if self.tubes[tube] == nil then
        error(('Tube %s not exist'):format(tube))
    end
    if self.tubes[tube].method[method] == nil then
        error(('Method %s not implemented in tube %s'):format(method, tube))
    end
    return self.tubes[tube].method[method](args)
end

local function map(self)
    return self.tubes
end

local function get_options(self, tube)
    return self.options[tube]
end

local function update(self, cfg_tubes)
    local existing_tubes = self:map()

    self.options = cfg_tubes or {}
    self.tubes = map_tubes(cfg_tubes)

    -- Create new.
    local new = {}
    for tube_name, driver in pairs(self.tubes) do
        if existing_tubes[tube_name] == nil then
            self.tubes[tube_name].create({
                name = tube_name,
                options = cfg_tubes[tube_name]
            })
            table.insert(new, tube_name)
        end
    end

    -- Remove old.
    local old = {}
    for tube_name, driver in pairs(existing_tubes) do
        if self.tubes[tube_name] == nil then
            driver.drop(tube_name)
            table.insert(old, tube_name)
        end
    end

    return new, old
end

local mt = {
    __index = {
        call = call,
        get_options = get_options,
        map = map,
        update = update,
    },
}

local function new()
    local ret = {
        tubes = {},
        options = {},
    }
    setmetatable(ret, mt)
    return ret
end

return {
    new = new,
}
