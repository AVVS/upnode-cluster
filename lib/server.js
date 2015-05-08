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

    var host = opts.host || '127.0.0.1';

    this.id = host + ':' + opts.port;
    this.log = Logger.child({ node_id: this.id, label: 'server' });
    this.server = net.createServer(this.onConnect);
    this.server.listen(opts.port, host, callback || _.noop);
}

Server.prototype.onConnect = function (stream) {
    this.log.info('incoming stream');
    var RPC = rpc(this.id);
    var up = upnode(function createRPCHandler(client, connection) {
        return new RPC(client, connection);
    });
    up.pipe(stream).pipe(up);
};

Server.prototype.close = function (callback) {
    this.server.close(callback || _.noop);
};

module.exports = Server;
