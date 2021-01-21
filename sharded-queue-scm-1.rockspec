package = 'sharded-queue'
version = 'scm-1'
source  = {
    url = 'git+https://github.com/lkwd/sharded-queue.git';
    branch = 'master';
}
dependencies = {
    'lua >= 5.1';
    'checks >= 3.0.0',
    'cartridge >= 2.0.0, < 3.0.0',
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
            ['sharded_queue.api'] = 'sharded_queue/api.lua',
            ['sharded_queue.storage'] = 'sharded_queue/storage.lua',
            ['sharded_queue.drivers.fifo'] = 'sharded_queue/drivers/fifo.lua',
            ['sharded_queue.drivers.fifottl'] = 'sharded_queue/drivers/fifottl.lua',
            ['sharded_queue.time'] = 'sharded_queue/time.lua',
            ['sharded_queue.utils'] = 'sharded_queue/utils.lua',
            ['sharded_queue.state'] = 'sharded_queue/state.lua',
            ['sharded_queue.statistics'] = 'sharded_queue/statistics.lua',
        },
    },
    build_variables = {
        version = 'scm-1',
    },
    install_pass = false,
}
