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

function utils.array_contains(array, value, key)
    if not array then
        return false
    end
    local key = key or function(x) return x end
    for _, v in ipairs(array) do
        if key(v) == value then
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

-- a < b
function utils.subset_of(a, b)
    for _, value in pairs(a) do
        if not utils.array_contains(b, value) then
            return false
        end
    end
    return true
end

-- a = b
function utils.equal_sets(a, b)
    return utils.subset_of(a, b) and utils.subset_of(b, a)
end

function utils.pack_task_id(bucket, bucket_count, index)
    return index * (bucket_count + 1) + bucket
end

function utils.unpack_task_id(task_id, bucket_count)
    local index = math.floor(task_id / (bucket_count + 1))
    local bucket = task_id - index * (bucket_count + 1)
    return bucket, index
end

return utils
