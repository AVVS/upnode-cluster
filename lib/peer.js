'use strict';

var Promise = require('bluebird');
var upnode = require('upnode');
var _ = require('lodash');
var assert = require('assert');
var Logger = require('./logger.js');
var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;
var Errors = require('node-common-errors');

function Peer(opts, callback) {

    EventEmitter.call(this);

    _.bindAll(this);

    callback = callback || _.noop;

    assert.ok(opts, 'opts must be defined');
    assert.ok(opts.port, 'opts.port must be defined');
    assert.ok(opts.host, 'opts.host must be defined');

    this.id = opts.host + ':' + opts.port;
    this.log = Logger.child({ node_id: this.id, label: 'peer' });
    var up = this.up = upnode.connect(opts, this.onConnect);

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

    this.up.on('up', this.reemitUp);
    this.up.on('down', this.reemitDown);
}
inherits(Peer, EventEmitter);

Peer.prototype.request = function (timeout) {
    var self = this;
    timeout = typeof timeout === 'number' ? timeout : 500;

    return Promise.fromNode(function upRequestPromise(callback) {
        self.log.debug('Requesting connection to peer %s', self.id);
        self.up(timeout, function upRequest(remote) {
            if (!remote) {
                self.log.warn('Connection not initialized in time to %s', self.id);
                return callback(new Errors.Uninitialized('peer ' + self.id + 'did not connect in time'));
            }

            callback(null, remote);
        });
    });
};

Peer.prototype.reemitDown = function () {
    this._connected = false;
    this.emit('down', this);
};

Peer.prototype.reemitUp = function () {
    this._connected = true;
    this.emit('up');
};

Peer.prototype.onConnect = function (remote, connection) {
    this.log.debug('establishing connection');
    this._remote = remote;
    this._conn = connection;

    connection.emit('up', remote);
};

Peer.prototype.close = function (callback) {
    try {
        this.up.removeListener('up', this.reemitUp);
        this.up.removeListener('down', this.reemitDown);
        this.up.close();
        this.reemitDown();
    } catch (e) {}

    setImmediate(callback);
};

module.exports = Peer;
