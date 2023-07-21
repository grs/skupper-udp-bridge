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

var http = require('http');
var kubernetes = require('../lib/kubernetes.js').client();
var log = require('../lib/log.js').logger();
var udp = require('../lib/udp.js');

function create_udp_connector (attributes) {
    console.log('create_udp_connector(%o)', attributes)
    var bridge = udp.egress(attributes.address, attributes.host, attributes.port, attributes.multicast);
    return bridge;
}

function create_udp_listener (attributes) {
    console.log('create_udp_listener(%o)', attributes)
    var bridge = udp.ingress(attributes.port, attributes.address, attributes.multicast);
    return bridge;
}

function Target(selector, address, ports, multicast) {
    console.log('Creating target with selector %s, address %s, ports %o and multicast %s', selector, address, ports, multicast);
    this.address = address;
    this.ports = ports;
    this.multicast = multicast;
    this.connectors = {};//host => array of connectors
    this.pod_watcher = kubernetes.watch('pods', undefined, selector);
    this.pod_watcher.on('updated', this.pods_updated.bind(this));
}

Target.prototype.pods_updated = function (pods) {
    var hosts = pods.filter(pod_ready_and_running).map(get_pod_ip);
    console.log('pods_updated(): hosts=', hosts);
    //for each host, create a connector for each port in service definition
    for (var i in hosts) {
        var host = hosts[i]
        if (!(host in this.connectors)) {
            var bridges = [];
            for (var key in this.ports) {
                var targetPort = this.ports[key];
                console.log('Creating connector for %s:%s', host, targetPort)
                bridges.push(create_udp_connector({port: targetPort, address: this.address + '_' + key, host: host, multicast: this.multicast}));
            }
            if (bridges.length) {
                this.connectors[host] = bridges;
            }
        }
    }
    //get rid of any stale connectors
    for (var host in this.connectors) {
        if (!hosts.includes(host)) {
            console.log('Deleting stale connector for ' + host)
            bridges = this.connectors[host]
            for (var bridge in bridges) {
                bridge.stop()
            }
            delete(this.connectors[host])
        }
    }
}

function asMap(ports) {
    var result = {}
    for (var i in ports) {
        var port = ports[i];
        result[port] = port;
    }
    return result
}

function getTargetPorts(targetPorts, servicePorts) {
    if (!targetPorts) {
        return asMap(servicePorts)
    }
    return targetPorts
}

function Server(svc_def) {
    //for each port, create a listener
    for (var i in svc_def.ports) {
        var port=svc_def.ports[i];
        create_udp_listener({port: port, address: svc_def.address + '_' + port, multicast: svc_def.eventchannel});
    }
    //for each target, watch for pods
    for (var i in svc_def.targets) {
        var target = svc_def.targets[i];
        if (target.selector != undefined && target.selector != "") {
            new Target(target.selector, svc_def.address, getTargetPorts(target.targetPorts, svc_def.ports), svc_def.eventchannel)
        } else if (target.service != undefined && target.service != "") {
            for (var key in target.targetPorts) {
                var targetPort = target.targetPorts[key];
                create_udp_connector({port: targetPort, address: svc_def.address + '_' + key, multicast: svc_def.eventchannel});
            }
        } else {
            console.log('Ignoring target with neither service nor selector defined')
        }
    }
    //start healthz listener
    var healthz = http.createServer(function (request, response) {
        if (request.method === 'GET') {
            response.statusCode = 200;
            response.end("ok\n");
        } else {
            response.statusCode = 405;
            response.end("invalid method\n");
        }
    });
    var healthPort = 9090;
    while (svc_def.ports.includes(healthPort)) {
        healthPort++;
    }
    console.log('Healthz served on port ' + healthPort);
    healthz.listen(healthPort, '0.0.0.0');
}

process.on('SIGTERM', function () {
    console.log('Exiting due to SIGTERM');
    process.exit();
});

function pod_ready (pod) {
    for (var i in pod.status.conditions) {
        if (pod.status.conditions[i].type === 'Ready') {
            return pod.status.conditions[i].status === 'True';
        }
    }
    return false;
}

function pod_running (pod) {
    return pod.status.phase === 'Running';
}

function pod_ready_and_running (pod) {
    return pod_ready(pod) && pod_running(pod);
}

function get_pod_ip (pod) {
    return pod.status.podIP;
}

var server = new Server(JSON.parse(process.env.SKUPPER_SERVICE_DEFINITION));
