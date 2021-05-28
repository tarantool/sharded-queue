local fiber = require('fiber')

local time = {}

time.MAX_TIMEOUT      = 365 * 86400 * 100       -- MAX_TIMEOUT == 100 years
time.DEDUPLICATION_TIME = 5 * 60 * 1e6          -- DEDUPLICATION_TIME == 5 minutes
time.TIMEOUT_INFINITY = 18446744073709551615ULL -- Set to TIMEOUT_INFINITY
time.MIN_NET_BOX_CALL_TIMEOUT = 1               -- MIN_NET_BOX_CALL_TIMEOUT == 1 second
time.DEFAULT_WAIT_FACTOR = 2

function time.sec(tm)
    if tm == nil then
        return
    end
    return tm / 1e6
end

function time.cur()
    return 0ULL + fiber.time64()
end

function time.nano(tm)
    if tm == nil then
        return
    end
    return 0ULL + tm * 1e6
end

function time.event(tm)
    if tm == nil or tm < 0 then
        tm = 0
    elseif tm > time.MAX_TIMEOUT then
        return time.TIMEOUT_INFINITY
    end
    tm = 0ULL + tm * 1000000 + fiber.time64()
    return tm
end

return time
