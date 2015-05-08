'use strict';

var upnode = require('upnode');
var net = require('net');
var _ = require('lodash');
var rpc = require('./rpc.js');
var assert = require('assert');
var Logger = require('./logger.js');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;

function Server(opts, callback) {

    _.bindAll(this);

    assert.ok(opts.port, 'opts.port must be defined');

    var self = this;
    var host = opts.host || '127.0.0.1';

    this.RPC = rpc(this);
    this.id = host + ':' + opts.port;
    this.log = Logger.child({ node_id: this.id, label: 'server' });
    this.server = net.createServer(this.onConnect);

    this.up = this.server.listen(opts.port, host, callback || function (err) {
        if (err) {
            self.emit('error', err);
            return;
        }

        self.emit('listen');
    });

    this._clients = [];
}
inherits(Server, EventEmitter);

Server.prototype.onConnect = function (stream) {
    var self = this;
    var up = upnode(this.RPC);
    up.pipe(stream).pipe(up);

    this._clients.push(up);
    up.once('end', function cleanupClientReference() {
        self._clients.splice(self._clients.indexOf(up), 1);
    });
};

Server.prototype.close = function (callback) {
    this._clients.forEach(function (client) {
        setImmediate(function () {
            client.end();
        });
    });
    try {
        this.server.close(callback);
    } catch (e) {
        setImmediate(callback);
    }
};

module.exports = Server;
