box.cfg{
    listen = 3013,
    wal_dir='m1/xlog',
    snap_dir='m1/snap',
}

box.once("init", function()
box.schema.user.create('test', {password = 'test'})
end)
