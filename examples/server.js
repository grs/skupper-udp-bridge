#!/usr/bin/env node

var dgram = require('dgram');
var server = dgram.createSocket('udp4');

server.on('error', function (err) {
    console.error(err);
    server.close();
});

server.on('message', function (msg, rinfo) {
    console.log('from %s:%s got %s', rinfo.address, rinfo.port, msg);
    server.send(msg, rinfo.port, rinfo.address);
});

server.on('listening', function () {
    var address = server.address();
    console.log('listening on %s:%s', address.address, address.port);
});

server.bind(process.env.PORT || 9292);
