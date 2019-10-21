#!/usr/bin/env tarantool

require('strict').on()
local log = require('log')
local cartridge = require('cartridge')
local console = require('console')

local binary_port = os.getenv('BINARY_PORT') or '3301'
local http_port = os.getenv('HTTP_PORT') or '8080'
local hostname = os.getenv('HOSTNAME') or 'localhost'
local console_sock = os.getenv("CONSOLE_SOCK")

local ok, err = cartridge.cfg({
    alias = os.getenv('ALIAS'),
    workdir = './dev/output-' .. binary_port,
    advertise_uri = hostname .. ':' .. binary_port,
    cluster_cookie = os.getenv('TARANTOOL_CLUSTER_COOKIE') or 'sharded-queue-cookie',
    bucket_count = 3000,
    http_port = http_port,
    roles = {
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
        'sharded_queue.storage',
        'sharded_queue.api'
    },
}, {
})

assert(ok, tostring(err))

if console_sock ~= nil then
    console.listen('unix/:' .. console_sock)
end
