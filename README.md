
# Tarantool Shared Queue Application

This application is an implementation of a distributed queue compatible with [Tarantool queue](https://github.com/tarantool/queue) (*fifiottl driver*)

## Running application

The script that starts and configures the cluster is located in `example`
Run it.
```
./example/test_configurate.sh
```
To stop and clear data, say:

```
./example/test_stop.sh
```
## Using

The queue api is located on all instances of the router masters that we launched.
For a test configuration, this is one router on `localhost:3301`

- **First way**  is to use `tarantooctl connect`
```lua
tarantool@user:~/shared_queue$ tarantoolctl connect localhost:3301
connected to localhost:3301 
localhost:3301> test_tube = shared_queue.create_tube('test_tube')
---
...
localhost:3301> test_tube
---
- tube_name: test_tube
...
localhost:3301> test_tube:put('task_1')
---
- [4990, 1989, 'r', 1566225784811962, 0, 3153600000000000, 3153600000000000, 4719825784811962,
  'task_1', 1]
...
localhost:3301> test_tube:take()        
---
- [4990, 1989, 't', 1566225784811962, 0, 3153600000000000, 3153600000000000, 4719825784811962,
  'task_1', 1]
...
```
- **Second way**  is to use `net.box`
```lua
tarantool@user:~/shared_queue$ tarantool
Tarantool Enterprise 1.10.3-6-gfbf53b9
type 'help' for interactive help
tarantool> netbox = require('net.box')
---
...
tarantool> queue_conn = netbox.connect('localhost:3301')
---
...
tarantool> queue_conn:eval('tube = shared_queue.create_tube(...)', { 'test_tube' })   
---
...
tarantool> queue_conn:call('tube:put', { 'task_1' })
---
- [3653, 652, 'r', 1566228200316049, 0, 3153600000000000, 3153600000000000, 4719828200316049,
  'task_1', 1]
...
tarantool> queue_conn:call('tube:take')
---
- [3653, 652, 't', 1566228200316049, 0, 3153600000000000, 3153600000000000, 4719828200316049,
  'task_1', 1]
...

```
## Launching tests

Say:

```
luatest
```
