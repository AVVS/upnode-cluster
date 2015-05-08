'use strict';

var upnode = require('upnode');
var net = require('net');
var _ = require('lodash');
var rpc = require('./rpc.js');
var assert = require('assert');
var Logger = require('./logger.js');

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
    this.up.close();
    setImmediate(callback);
};

module.exports = Server;
