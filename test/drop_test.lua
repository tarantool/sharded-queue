local netbox = require('net.box')

local t = require('luatest')

local g = t.group('drop_test')

---

g.before_all = function()
    queue_conn = netbox.connect('localhost:3301')
end

g.after_all = function()
    queue_conn:close()
end


---

function g.test_drop_empty()
    local tube_name = 'drop_empty_test'
    
    queue_conn:eval('tube = shared_queue.create_tube(...)', { tube_name })
    queue_conn:call('tube:drop')
    cur_stat = queue_conn:call('shared_queue.statistics', { tube_name })
    t.assert_equals(cur_stat, nil)
end