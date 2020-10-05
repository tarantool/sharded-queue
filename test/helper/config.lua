local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
require('json').cfg { encode_use_tostring = true }

local config = {}

config.root = fio.dirname(fio.abspath(package.search('init')))

config.datadir = fio.pathjoin(config.root, 'dev')
config.unitdir = fio.pathjoin(config.datadir, 'unit')

config.cluster = cartridge_helpers.Cluster:new({
    datadir = config.datadir,
    server_command = fio.pathjoin(config.root, 'init.lua'),
    use_vshard = true,
    replicasets = {
        {
            uuid = 'aaaaaaaa-0000-4000-b000-000000000000',
            roles = {
                'sharded_queue.api'
            },
            servers = {
                {
                    instance_uuid = 'aaaaaaaa-aaaa-4000-b000-000000000001',
                    alias = 'queue-router',
                    advertise_port = 3301,
                    cluster_cookie = 'sharded-queue-cookie',
                },
                {
                    instance_uuid = 'aaaaaaaa-aaaa-4000-b000-000000000002',
                    alias = 'queue-router-1',
                    advertise_port = 3304,
                    http_port = 8084,
                    cluster_cookie = 'sharded-queue-cookie',
                }
            },
        },
        {
            uuid = 'bbbbbbbb-0000-4000-b000-000000000000',
            roles = {
                'sharded_queue.storage'
            },
            servers = {
                {
                    instance_uuid = 'bbbbbbbb-bbbb-4000-b000-000000000001',
                    alias = 'queue-storage-1',
                    advertise_port = 3302,
                    http_port = 8082,
                    cluster_cookie = 'sharded-queue-cookie',
                },
            }
        },
        {
            uuid = 'cccccccc-0000-4000-b000-000000000000',
            roles = {'sharded_queue.storage'},
            servers = {
                {
                    instance_uuid = 'cccccccc-cccc-4000-b000-000000000001',
                    alias = 'queue-storage-2',
                    advertise_port = 3303,
                    http_port = 8083,
                    cluster_cookie = 'sharded-queue-cookie',
                }
            },
        }
    }
})

t.before_suite(function ()
    fio.rmtree(config.datadir)
    fio.mktree(config.datadir)
    config.cluster:start()

    fio.mktree(config.unitdir)
    box.cfg{
        work_dir=config.unitdir,
        wal_mode='none'
    }
end)

t.after_suite(function () config.cluster:stop() end)

return config
