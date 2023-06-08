#!/usr/bin/env tarantool

local cartridge = require('cartridge')

local ok, err = cartridge.cfg({
    bucket_count = 3000,
    roles = {
        'sharded_queue.storage',
        'sharded_queue.api'
    },
}, {
})

assert(ok, tostring(err))
