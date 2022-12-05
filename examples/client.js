#!/usr/bin/env node

var dgram = require('dgram');
var client = dgram.createSocket('udp4');

client.on('error', function (err) {
    console.error(err);
    client.close();
});

client.on('message', function (msg, rinfo) {
    console.log('from %s:%s got %s', rinfo.address, rinfo.port, msg);
});

client.on('listening', function () {
    var address = client.address();
    console.log('listening on %s:%s', address.address, address.port);
});

client.bind(0);

var port = process.argv[2] || 9292;
var host = process.argv[3] || 'localhost';

process.stdin.on('data', function (data) {
    client.send(data.toString().trim(), port, host);
}).on('end', function () {
    client.close();
});

