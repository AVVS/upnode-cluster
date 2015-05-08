'use strict';

var upnode = require('upnode');
var _ = require('lodash');
var assert = require('assert');
var Logger = require('./logger.js');

function Peer(opts, callback) {

    _.bindAll(this);

    callback = callback || _.noop;

    assert.ok(opts, 'opts must be defined');
    assert.ok(opts.port, 'opts.port must be defined');
    assert.ok(opts.host, 'opts.host must be defined');

    this.id = opts.host + ':' + opts.port;
    this.log = Logger.child({ node_id: this.id, label: 'peer' });
    var up = this.up = upnode.connect(opts.port, opts.host, this.onConnect);

    function onConnectionEstablished(errOrRemote) {
        if (errOrRemote instanceof Error) {
            up.removeListener('up', onConnectionEstablished);
            return callback(errOrRemote);
        }

        // err equals remote
        up.removeListener('error', onConnectionEstablished);
        callback(null, errOrRemote);
    }

    this.up.once('up', onConnectionEstablished);
    this.up.once('error', onConnectionEstablished);
}

Peer.prototype.onConnect = function (remote, connection) {
    this.log.debug('establishing connection');
    this._remote = remote;
    this._conn = connection;

    connection.emit('up', remote);
};

Peer.prototype.close = function (callback) {
    this.up.close();
    callback();
};

module.exports = Peer;
