box.cfg{
    listen = 3013,
    wal_dir='xlog',
    snap_dir='snap',
}

box.once("init", function()
local s = box.schema.space.create('test', {
    id = 512,
    if_not_exists = true,
})
s:create_index('primary', {type = 'tree', parts = {1, 'NUM'}, if_not_exists = true})

local st = box.schema.space.create('schematest', {
    id = 514,
    temporary = true,
    if_not_exists = true,
    field_count = 7,
    format = {
        [2] = {name = "name1"},
        [3] = {type = "type2"},
        [6] = {name = "name5", type = "str", more = "extra"},
    },
})
st:create_index('primary', {
    type = 'hash', 
    parts = {1, 'NUM'}, 
    unique = true,
    if_not_exists = true,
})
st:create_index('secondary', {
    id = 3,
    type = 'tree',
    unique = false,
    parts = { 2, 'num', 3, 'STR' },
    if_not_exists = true,
})
st:truncate()

--box.schema.user.grant('guest', 'read,write,execute', 'universe')
box.schema.func.create('box.info')
box.schema.func.create('simple_incr')

-- auth testing: access control
box.schema.user.create('test', {password = 'test'})
box.schema.user.grant('test', 'execute', 'universe')
box.schema.user.grant('test', 'read,write', 'space', 'test')
box.schema.user.grant('test', 'read,write', 'space', 'schematest')
end)

function simple_incr(a)
    return a+1
end

box.space.test:truncate()
local console = require 'console'
console.listen '0.0.0.0:33015'

--box.schema.user.revoke('guest', 'read,write,execute', 'universe')

