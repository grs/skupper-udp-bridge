/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

var dgram = require('dgram');
var rhea = require('rhea');
var util = require('util');

var client_ip = require('./utils.js').client_ip;

var LOG_DATA = process.env.LOG_DATA;
var LOG_MAPPING = process.env.LOG_MAPPING;

function Pool(min_size, factory) {
    this.min_size = min_size;
    this.factory = factory;
    this.items = [];
    this.replenish();
}

Pool.prototype.clear = function () {
    this.items = [];
};

Pool.prototype.replenish = function() {
    while (this.items.length < this.min_size) {
        this.items.push(this.factory());
    }
};

Pool.prototype.take = function() {
    if (this.items.length > 0) {
        var item = this.items.shift();
        this.replenish();
        return item;
    } else {
        return this.factory();
    }
};

Pool.prototype.put = function(item) {
    this.items.push(item);
};

function has_expired(object, max_unused) {
    return (Date.now() - object.last_used) > max_unused;
}

function Subscriber(mappings, receiver, udp_address) {
    this.mappings = mappings;
    this.receiver = receiver;
    this.receiver.on('message', this.on_message.bind(this));
    this.address_promise = new Promise(function (resolve, reject) {
        receiver.on('receiver_open', function (context) {
            resolve(context.receiver.source.address);
        });
    });
    this.last_used = Date.now();
    this.udp_address = udp_address;
}

Subscriber.prototype.get_amqp_address = function () {
    this.last_used = Date.now();
    return this.address_promise;
};

Subscriber.prototype.on_message = function (context) {
    this.last_used = Date.now();
    var data = context.message.body;
    var reply_to = context.message.reply_to;
    var relay = this.mappings.get_udp_relay(reply_to)
    relay.send(data, this.udp_address.port, this.udp_address.address);
    if (LOG_DATA) console.log('%s SEND %s -> %s:%s | %s -> %s [%s]', new Date().toISOString(), relay.udp_address.port, this.udp_address.address, this.udp_address.port, reply_to, context.receiver.source.address, data.toString('hex'));
};

Subscriber.prototype.map = function (udp_address) {
    this.last_used = Date.now();
    this.udp_address = udp_address;
    this.mappings.map_address(udp_address, this);
    if (LOG_MAPPING) {
        this.get_amqp_address().then(function (amqp_address) {
            console.log('%s MAP %s -> %s:%s', new Date().toISOString(), amqp_address, udp_address.address, udp_address.port);
        });
    }
};

Subscriber.prototype.unmap = function () {
    this.mappings.unmap_address(this.udp_address);
    if (LOG_MAPPING) console.log('%s UNMAP %s -> %s:%s', new Date().toISOString(), this.receiver.source.address, this.udp_address.address, this.udp_address.port);
    this.udp_address = undefined;
};

//a UDP socket associated with an AMQP address over which received UDP messages are sent
function Relay(mappings, port, sender, address) {
    this.mappings = mappings;
    this.sender = sender;
    this.amqp_address = address;
    this.socket = dgram.createSocket('udp4');
    this.socket.on('listening', this.on_listening.bind(this));
    this.socket.on('message', this.on_message.bind(this));
    this.socket.bind(port);
    this.last_used = Date.now();
}

Relay.prototype.on_message = function (data, rinfo) {
    this.last_used = Date.now();
    var self = this;
    this.mappings.get_reply_address(rinfo).then (function (reply_address) {
        var msg = {
            reply_to: reply_address,
            body: data
        };
        if (LOG_DATA) console.log('%s RECV %s:%s ->  %s | %s -> %s [%s]', new Date().toISOString(), rinfo.address, rinfo.port, self.udp_address.port, reply_address, self.amqp_address, data.toString('hex'));
        self.sender.send(msg);
    });
};

Relay.prototype.send = function (data, port, address) {
    this.last_used = Date.now();
    this.socket.send(data, port, address);
};

Relay.prototype.on_listening = function () {
    this.udp_address = this.socket.address();
};

Relay.prototype.map = function (amqp_address) {
    this.last_used = Date.now();
    this.amqp_address = amqp_address;
    this.sender = this.mappings.connection.open_sender(amqp_address);
    this.mappings.map_relay(amqp_address, this);
    if (LOG_MAPPING) console.log('%s MAP %s:%s -> %s', new Date().toISOString(), this.udp_address.address, this.udp_address.port, this.amqp_address);
};

Relay.prototype.unmap = function () {
    this.sender.close();
    this.mappings.unmap_relay(this.amqp_address);
    if (LOG_MAPPING) console.log('%s UNMAP %s:%s -> %s', new Date().toISOString(), this.udp_address.address, this.udp_address.port, this.amqp_address);
    this.amqp_address = undefined;
};

function udp_address_key(address) {
    return util.format("%s:%s", address.address, address.port);
}

function unmap_expired(items, max_unused) {
    for (var key in items) {
        if (has_expired(items[key], max_unused)) {
            if (LOG_MAPPING) console.log('%s expiring %s', new Date().toISOString(), key);
            items[key].unmap();
        }
    }
}

function unmap_all(items, max_unused) {
    for (var key in items) {
        items[key].unmap();
    }
}

function Mappings (connection) {
    this.connection = connection;
    this.address_mappings = {}; //UDP address to AMQP dynamic receiver
    this.pooled_receivers = new Pool(5, this.new_dynamic_receiver.bind(this));
    this.udp_relays = {}; //AMQP reply address to UDP relay
    this.pooled_relays = new Pool(5, this.new_udp_relay.bind(this));
    this.max_unused = (process.env.TTL_SECONDS | 60) * 1000;
    this.connection.on('disconnect', this.unmap_all.bind(this));
    this.timer = setInterval(this.unmap_expired.bind(this), (process.env.EXPIRATION_CHECK_INTERVAL_SECONDS | 10)*1000);
}

Mappings.prototype.stop = function () {
    clearInterval(this.timer);
    this.unmap_all();
    this.pooled_receivers.clear();
    this.pooled_relays.clear();
};

Mappings.prototype.map_address = function (udp_address, receiver) {
    this.address_mappings[udp_address_key(udp_address)] = receiver;
};

Mappings.prototype.unmap_address = function (udp_address) {
    var key = udp_address_key(udp_address);
    this.pooled_receivers.put(this.address_mappings[key]);
    delete this.address_mappings[key];
};

Mappings.prototype.map_relay = function (amqp_address, relay) {
    this.udp_relays[amqp_address] = relay;
};

Mappings.prototype.unmap_relay = function (amqp_address) {
    this.pooled_relays.put(this.udp_relays[amqp_address]);
    delete this.udp_relays[amqp_address];
};

Mappings.prototype.unmap_expired = function () {
    unmap_expired(this.address_mappings, this.max_unused);
    unmap_expired(this.udp_relays, this.max_unused);
};

Mappings.prototype.unmap_all = function () {
    unmap_all(this.address_mappings, this.max_unused);
    unmap_all(this.udp_relays, this.max_unused);
};

Mappings.prototype.new_dynamic_receiver = function () {
    return new Subscriber(this, this.connection.open_receiver({source:{dynamic:true}}));
};

Mappings.prototype.new_udp_relay = function () {
    return new Relay(this, 0);
};

Mappings.prototype.get_reply_address = function (udp_address) {
    var key = udp_address_key(udp_address);
    var receiver = this.address_mappings[key];
    if (!receiver) {
        //need to allocate a dynamic amqp address
        receiver = this.pooled_receivers.take()
        receiver.map(udp_address);
    }
    return receiver.get_amqp_address();
};

Mappings.prototype.get_udp_relay = function (reply_to) {
    var relay = this.udp_relays[reply_to];
    if (!relay) {
        //need to create and map a relay
        relay = this.pooled_relays.take();
        relay.map(reply_to);
    }
    return relay
};


function service_address(address, multicast) {
    if (multicast) {
        return 'mc/' + address;
    } else {
        return address;
    }
}

function UdpToAmqpBridge(port, address, multicast) {
    this.port = port;
    this.address = address;
    this.multicast = multicast;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_udp_' + this.port + '_' + this.address;
    this.connection = container.connect();
    this.mappings = new Mappings(this.connection);
    var address = service_address(address, multicast)
    var sender = this.connection.open_sender(address);
    this.relay = new Relay(this.mappings, port, sender, address)
}

UdpToAmqpBridge.prototype.stop = function () {
    this.mappings.stop();
    this.connection.close();
};

function AmqpToUdpBridge(address, host, port, multicast) {
    this.address = address;
    this.port = port;
    this.host = host;
    this.multicast = multicast;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_udp_' + this.address + '_' + this.port;
    this.connection = container.connect();
    this.mappings = new Mappings(this.connection);
    this.subscriber = new Subscriber(this.mappings, this.connection.open_receiver(service_address(address, multicast)), {address: host, port: port})
}

AmqpToUdpBridge.prototype.stop = function () {
    this.mappings.stop();
    this.connection.close();
};

module.exports.ingress = function (port, address, multicast) {
    if (multicast) {
        return new UdpToAmqpBridge(port, address, multicast);
    } else {
        return new Listener(port, address);
    }
};

module.exports.egress = function (address, host, port, multicast) {
    return new AmqpToUdpBridge(address, host, port, multicast);
};

//Listener establishes a time based sticky session between a 'client'
//and a 'server' process
function Listener(port, address) {
    this.port = port;
    this.amqp_address = address;

    this.receivers = {}; //UDP address to AMQP dynamic receiver
    this.sessions = {}; //UDP address to AMQP sender

    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_udp_' + this.port + '_' + this.address;
    this.connection = container.connect();
    this.sender = this.connection.open_sender(this.amqp_address);
    this.socket = dgram.createSocket('udp4');
    this.socket.on('listening', this.on_listening.bind(this));
    this.socket.on('message', this.on_message.bind(this));
    this.socket.bind(port);
    this.max_unused = (process.env.TTL_SECONDS | 60) * 1000;
    this.connection.on('disconnect', this.unmap_all.bind(this));
    this.timer = setInterval(this.unmap_expired.bind(this), (process.env.EXPIRATION_CHECK_INTERVAL_SECONDS | 10)*1000);

    this.pooled_receivers = new Pool(5, this.new_dynamic_receiver.bind(this));
};

Listener.prototype.stop = function () {
    clearInterval(this.timer);
    this.unmap_all();
    this.pooled_receivers.clear();
    this.connection.close();
};

Listener.prototype.unmap_expired = function () {
    unmap_expired(this.receivers, this.max_unused);
    unmap_expired(this.sessions, this.max_unused);
};

Listener.prototype.unmap_all = function () {
    unmap_all(this.receivers, this.max_unused);
    unmap_all(this.sessions, this.max_unused);
};

Listener.prototype.on_listening = function () {
    this.udp_address = this.socket.address();
};

Listener.prototype.on_message = function (data, rinfo) {
    this.last_used = Date.now();
    var self = this;
    this.get_reply_address(rinfo).then (function (reply_address) {
        var msg = {
            reply_to: reply_address,
            body: data
        };
        var session = self.sessions[udp_address_key(rinfo)];
        if (session) {
            if (LOG_DATA) console.log('%s RECV %s:%s ->  %s | %s -> %s [%s]', new Date().toISOString(), rinfo.address, rinfo.port, self.udp_address.port, reply_address, session.amqp_address, data.toString('hex'));
            session.send(msg)
        } else {
            if (LOG_DATA) console.log('%s RECV %s:%s ->  %s | %s -> %s [%s]', new Date().toISOString(), rinfo.address, rinfo.port, self.udp_address.port, reply_address, self.amqp_address, data.toString('hex'));
            self.sender.send(msg);
        }
    });
};

Listener.prototype.get_reply_address = function (udp_address) {
    var key = udp_address_key(udp_address);
    var receiver = this.receivers[key];
    if (!receiver) {
        //need to allocate a dynamic amqp address
        receiver = this.pooled_receivers.take()
        this.map_receiver(udp_address, receiver);
    }
    return receiver.get_amqp_address();
};

Listener.prototype.new_dynamic_receiver = function () {
    return new Receiver(this, this.connection.open_receiver({source:{dynamic:true}}));
};

Listener.prototype.map_receiver = function (udp_address, receiver) {
    receiver.last_used = Date.now();
    receiver.udp_address = udp_address;
    this.receivers[udp_address_key(udp_address)] = receiver;
    if (LOG_MAPPING) {
        receiver.get_amqp_address().then(function (amqp_address) {
            console.log('%s MAP %s -> %s:%s', new Date().toISOString(), amqp_address, udp_address.address, udp_address.port);
        });
    }
};

Listener.prototype.unmap_receiver = function (receiver) {
    delete this.receivers[udp_address_key(receiver.udp_address)];
    receiver.udp_address = undefined;
    this.pooled_receivers.put(receiver);
};

Listener.prototype.map_session = function (udp_address, amqp_address) {
    var key = udp_address_key(udp_address);
    var session = this.sessions[key];
    if (session) {
        session.touch();
    } else {
        session = new Session(this, this.connection.open_sender(amqp_address), udp_address, amqp_address);
        this.sessions[key] = session;
        console.log('%s SESSION ESTABLISHED %s:%s <-> %s', new Date().toISOString(), udp_address.address, udp_address.port, amqp_address);
    }
};

Listener.prototype.unmap_session = function (session) {
    var key = udp_address_key(session.udp_address);
    var current = this.sessions[key];
    if (current && current == session) {
        console.log('%s SESSION ENDED %s:%s <-> %s', new Date().toISOString(), session.udp_address.address, session.udp_address.port, session.amqp_address);
        delete this.sessions[key];
    } else {

    }
};

Listener.prototype.unmap_all = function () {
    for (var key in this.receivers) {
        this.receivers[key].unmap();
    }
    for (var key in this.sessions) {
        this.sessions[key].unmap();
    }
};

Listener.prototype.send = function (data, port, address) {
    this.socket.send(data, port, address);
};

function Receiver(listener, receiver) {
    this.listener = listener;
    this.receiver = receiver;
    this.receiver.on('message', this.on_message.bind(this));
    var self = this;
    this.address_promise = new Promise(function (resolve, reject) {
        receiver.on('receiver_open', function (context) {
            resolve(context.receiver.source.address);
        });
    });
    this.last_used = Date.now();
};

Receiver.prototype.get_amqp_address = function () {
    this.last_used = Date.now();
    return this.address_promise;
};

Receiver.prototype.on_message = function (context) {
    this.last_used = Date.now();
    var data = context.message.body;
    var reply_to = context.message.reply_to;
    this.listener.map_session(this.udp_address, reply_to)
    this.listener.send(data, this.udp_address.port, this.udp_address.address);
    if (LOG_DATA) console.log('%s SEND %s -> %s:%s | %s -> %s [%s]', new Date().toISOString(), this.listener.udp_address.port, this.udp_address.address, this.udp_address.port, reply_to, context.receiver.source.address, data.toString('hex'));
};

Receiver.prototype.unmap = function () {
    this.listener.unmap_receiver(this)
};

function Session(listener, sender, udp_address, amqp_address) {
    this.listener = listener;
    this.sender = sender;
    this.sender.on('released', this.on_released.bind(this));
    this.udp_address = udp_address;
    this.amqp_address = amqp_address;
    this.last_used = Date.now();
};

Session.prototype.touch = function () {
    this.last_used = Date.now();
};

Session.prototype.send = function (message) {
    this.touch();
    this.sender.send(message)
};

Session.prototype.unmap = function () {
    this.sender.close();
    this.listener.unmap_session(this);
};

Session.prototype.on_released = function (context) {
    console.log('%s MESSAGE RELEASED; ENDING SESSION %s:%s <-> %s', new Date().toISOString(), this.udp_address.address, this.udp_address.port, this.amqp_address);
    this.unmap();
};
