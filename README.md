
# Tarantool Sharded Queue Application

This module provides cartridge roles implementing of a distributed queue compatible with [Tarantool queue](https://github.com/tarantool/queue) (*fifiottl driver*)

## Installing

1. Add dependency to your application rockspec
2. Add roles to your application:
```init.lua
cartridge.cfg({
    ...
    roles = {        
        'sharded_queue.storage',
        'sharded_queue.api'
        ...
    },
    ...
})
```
3. Queue API will be available on all nodes where sharded_queue.api is enabled

## Using

The good old queue api is located on all instances of the router masters that we launched.
For a test configuration, this is one router on `localhost:3301`

```
tarantool@user:~/sharded_queue$ tarantool
Tarantool 1.10.3-6-gfbf53b9
type 'help' for interactive help
tarantool> netbox = require('net.box')
---
...
tarantool> queue_conn = netbox.connect('localhost:3301', {user = 'admin',password = 'sharded-queue-cookie'})
---
...
tarantool> queue_conn:call('queue.create_tube', { 'test_tube' })   
---
...
tarantool> queue_conn:call('queue.tube.test_tube:put', { 'task_1' })
---
- [3653, 652, 'r', 1566228200316049, 0, 3153600000000000, 3153600000000000, 4719828200316049,
  'task_1', 1]
...
tarantool> queue_conn:call('queue.tube.test_tube:take')
---
- [3653, 652, 't', 1566228200316049, 0, 3153600000000000, 3153600000000000, 4719828200316049,
  'task_1', 1]
...

```

You may also set up tubes using cluster-wide config:
```config.yml
tubes:
     tube_1:
        temporary: true
        ttl: 60
     tube_2:
        driver: my_app.my_driver
```

## Running locally

Install dependencies:

```
tarantoolctl rocks make
```

The script that starts and configures the cartridge is located in `example`
Run it.
```
./example/configurate.sh
```
To stop and clear data, say:

```
./example/stop.sh
```

## Launching tests
    
Say:

```
tarantoolctl rocks install luatest
.rocks/bin/luatest -v
```

## API extensions (compared to tarantool/queue)

* ``tube:take`` method has additional table argument ``options``. It may be used to provide additional logic in some
    drivers.
    
    Example:
    ```lua
      tube:take(3, {cumstom_driver_option='test'})
    ```
