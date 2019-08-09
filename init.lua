#!/usr/bin/env tarantool

require('strict').on()
local log = require('log')
local cluster = require('cluster')
local console = require('console')
local space_explorer = require('space-explorer')

local binary_port = os.getenv('BINARY_PORT') or '3301'
local http_port = os.getenv('HTTP_PORT') or '8080'
local hostname = os.getenv('HOSTNAME') or 'localhost'
local console_sock = os.getenv("CONSOLE_SOCK")

local ok, err = cluster.cfg({
    alias = os.getenv('ALIAS'),
    workdir = './dev/output-' .. binary_port,
    advertise_uri = hostname .. ':' .. binary_port,
    cluster_cookie = 'secret-cluster-cookie',
    bucket_count = 10,
    http_port = http_port,
    roles = {
        'cluster.roles.vshard-storage',
        'cluster.roles.vshard-router',
        'queue.storage',
        'queue.api',
    },
}, {
    -- put your box.cfg arguments here
    log = './dev/output-' .. binary_port .. '/main.log',
    audit_log = './dev/output-' .. binary_port .. '/audit.log',
})

space_explorer.init()

assert(ok, tostring(err))

if console_sock ~= nil then
    console.listen('unix/:' .. console_sock)
end
