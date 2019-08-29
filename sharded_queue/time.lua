local fiber = require('fiber')

local time = {}

time.MAX_TIMEOUT      = 365 * 86400 * 100       -- MAX_TIMEOUT == 100 years
time.TIMEOUT_INFINITY = 18446744073709551615ULL -- Set to TIMEOUT_INFINITY

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

function time.time(tm)
    if tm == nil then
        tm = fiber.time64()
    elseif tm < 0 then
        tm = 0
    else
        tm = tm * 1e6
    end
    return 0ULL + tm
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