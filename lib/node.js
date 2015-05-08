'use strict';

var Server = require('./server.js');
var Peer = require('./peer.js');
var Logger = require('./logger.js');
var DistributedQueue = require('distributed-callback-queue');
var Redis = require('ioredis');
var _ = require('lodash');
var assert = require('assert');

function Node(opts, callback) {

    _.bindAll(this);

    assert.ok(opts, 'opts must be defined');
    assert.ok(opts.redis, 'opts.redis configuration must be defined');
    assert.ok(opts.pubsubChannel, 'opts.pubsubChannel must be defined');
    assert.ok(opts.cluster, 'opts.cluster must be defined');
    assert.ok(opts.server || opts.peer, 'at least opts.server or opts.peer must be defined');

    this.clusterName = opts.cluster;
    this._announceChannel = this.clusterName + '~announce';
    this._options = opts;

    this._redis = opts.redis instanceof Redis ? opts.redis : new Redis(opts.redis);
    this._pubsub = this._redis.duplicate();
    this._queue = new DistributedQueue({
        client: this._redis,
        pubsub: this._pubsub,
        pubsubChannel: opts.pubsubChannel,
        lock: {
            timeout: 10000,
            retries: 0,
        },
        lockPrefix: opts.cluster,
        log: false
    });

    this.log = Logger.child({ cluster_name: opts.cluster, server: !!opts.server, peer: !!opts.peer });

    if (opts.peer) {
        this._setupPeers();
    }

    if (opts.server) {
        this.server = new Server(opts.server, callback);
        this._setupServer(this.server, opts.server);
    } else {
        callback();
    }

}

/**
 * Sets up discoverd peer connections
 * @param {Object} configuration
 */
Node.prototype._setupPeers = function () {
    this._peers = {};
    this._pubsub.subscribe(this._announceChannel);
    this._pubsub.on('messageBuffer', this._discoveredServer);
};

Node.prototype._discoveredServer = function (channel, message) {
    if (channel.toString() !== this._announceChannel) {
        return;
    }

    var parsed;
    try {
        parsed = JSON.parse(message);
    } catch (e) {
        return;
    }

    if (parsed.cluster !== this.clusterName) {
        return;
    }

    var host = parsed.host;
    var port = parsed.port;

    if (!host || !port) {
        return;
    }

    var connectionString = host + ':' + port;
    if (this._peers[connectionString] || (this.server && connectionString === this.server.id)) {
        return;
    }

    this._peers[connectionString] = new Peer(parsed);
};

/**
 * Establishes server discovery via redis pubsub
 * @param {Object} server
 * @param {Object} configuration
 */
Node.prototype._setupServer = function (server, configuration) {
    var redis = this._redis;
    var announceChannel = this._announceChannel;
    var message = JSON.stringify({
        cluster: this.clusterName,
        host: configuration.host || '127.0.0.1',
        port: configuration.port
    });

    this._announceInterval = setInterval(function announce() {
        redis.publish(announceChannel, message);
    }, 250);
};

/**
 * Closes node gracefully
 * @param  {Function} callback
 */
Node.prototype.close = function (callback) {
    var called = false;
    var callbacksToBeCalled = 1;
    var redis = this._redis;
    var pubsub = this._pubsub;

    function onClosed(err) {
        if (called) {
            return;
        }

        if (err) {
            called = true;
            setImmediate(callback, err);
        } else if (--callbacksToBeCalled === 0) {
            called = true;
            setImmediate(callback);
        }

        if (called) {
            redis.disconnect();
            pubsub.disconnect();
        }
    }

    if (this._options.server) {
        clearInterval(this._announceInterval);
        callbacksToBeCalled++;
        this.server.close(onClosed);
    }

    if (this._options.peer && this._peers) {
        pubsub.removeListener('messageBuffer', this._discoveredServer);
        this._peers.forEach(function (peer) {
            callbacksToBeCalled++;
            peer.close(onClosed);
        });
    }

    onClosed();
};

module.exports = Node;
