package = 'sharded-queue'
version = 'scm-1'
source  = {
    url = 'git+https://github.com/tarantool/sharded-queue.git';
    branch = 'master';
}
dependencies = {
    'lua >= 5.1',
    'vshard >= 0.1.26-1',
}

external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h';
    };
}

build = {
    type = 'make',
	build_target = 'all',
    install = {
        lua = {
            ["roles.sharded-queue-router"] = "roles/sharded-queue-router.lua",
            ["roles.sharded-queue-storage"] = "roles/sharded-queue-storage.lua",
            ['sharded_queue.api'] = 'sharded_queue/api.lua',
            ['sharded_queue.storage'] = 'sharded_queue/storage.lua',
            ['sharded_queue.drivers.fifo'] = 'sharded_queue/drivers/fifo.lua',
            ['sharded_queue.drivers.fifottl'] = 'sharded_queue/drivers/fifottl.lua',
            ['sharded_queue.time'] = 'sharded_queue/time.lua',
            ['sharded_queue.utils'] = 'sharded_queue/utils.lua',
            ['sharded_queue.metrics'] = 'sharded_queue/metrics.lua',
            ['sharded_queue.roles'] = 'sharded_queue/roles.lua',
            ['sharded_queue.stash'] = 'sharded_queue/stash.lua',
            ['sharded_queue.state'] = 'sharded_queue/state.lua',
            ['sharded_queue.stats.storage'] = 'sharded_queue/stats/storage.lua',
            ['sharded_queue.router.config'] = 'sharded_queue/router/config.lua',
            ['sharded_queue.router.metrics'] = 'sharded_queue/router/metrics.lua',
            ['sharded_queue.router.queue'] = 'sharded_queue/router/queue.lua',
            ['sharded_queue.router.tube'] = 'sharded_queue/router/tube.lua',
            ['sharded_queue.storage.config'] = 'sharded_queue/storage/config.lua',
            ['sharded_queue.storage.drivers'] = 'sharded_queue/storage/drivers.lua',
            ['sharded_queue.storage.methods'] = 'sharded_queue/storage/methods.lua',
            ['sharded_queue.storage.metrics'] = 'sharded_queue/storage/metrics.lua',
            ['sharded_queue.storage.tubes'] = 'sharded_queue/storage/tubes.lua',
            ['sharded_queue.storage.vshard_utils'] = 'sharded_queue/storage/vshard_utils.lua',
            ['sharded_queue.version'] = 'sharded_queue/version.lua',
        },
    },
    build_pass = false,
    install_pass = false,
}
