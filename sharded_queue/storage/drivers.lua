local DEFAULT_DRIVER = 'sharded_queue.drivers.fifottl'
local queue_drivers = {}

local function get_driver(driver_name)
    driver_name = driver_name or DEFAULT_DRIVER

    if queue_drivers[driver_name] == nil then
        queue_drivers[driver_name] = require(driver_name)
    end
    return queue_drivers[driver_name]
end

return {
    get = get_driver,
}
