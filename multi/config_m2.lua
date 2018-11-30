box.cfg{
    listen = 3014,
    wal_dir='m2/xlog',
    snap_dir='m2/snap',
}

box.once("init", function()
box.schema.user.create('test', {password = 'test'})
end)
