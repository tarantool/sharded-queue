local fiber = require('fiber')

local utils = {}

function utils.array_shuffle(array)
    if not array then
        return nil
    end
    
    math.randomseed(tonumber(0ULL + fiber.time64()))

    for i = #array, 1, -1 do
        local j = math.random(i)
        array[i], array[j] = array[j], array[i]
    end
end

function utils.array_contains(array, value)
    if not array then
        return false
    end

    for _, v in ipairs(array) do
        if v == value then
            return true
        end
    end

    return false
end

function utils.array_max(array)
    if not array then
        return nil
    end

    local index, max = 1, array[1]
    for i, value in ipairs(array) do
        if array[i] > max then
            index, max = i, value
        end
    end
    return index, max
end

return utils