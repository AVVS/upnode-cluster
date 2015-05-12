'use strict';

var Promise = require('bluebird');
var DistributedQueue = require('distributed-callback-queue');
var Redis = require('ioredis');
var _ = require('lodash');
var assert = require('assert');
var Errors = require('node-common-errors');

var Server = require('./server.js');
var Peer = require('./peer.js');
var Logger = require('./logger.js');
var resource = require('./resource.js');

function Node(opts, callback) {

    _.bindAll(this);

    assert.ok(opts, 'opts must be defined');
    assert.ok(opts.redis, 'opts.redis configuration must be defined');
    assert.ok(opts.pubsubChannel, 'opts.pubsubChannel must be defined');
    assert.ok(opts.cluster, 'opts.cluster must be defined');
    assert.ok(opts.server || opts.peer, 'at least opts.server or opts.peer must be defined');

    this.clusterName = opts.cluster;
    this._announceChannel = this.clusterName + '~announce';
    this._resourceChannel = this.clusterName + '~resource';
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
        this._setupPeers(opts.peer);
    }

    if (opts.server) {
        this.server = new Server(opts.server, callback);
        this._setupServer(this.server, opts.server);
    } else {
        this._setupResourceScavenger = _.noop;
        callback();
    }

}

Node.prototype.resourceKey = function () {
    var length = arguments.length;
    var args = new Array(length);
    for (var i = 0; i < length; i++) {
        args[i] = arguments[i];
    }
    return args.join('~');
};

/**
 * This is helpful if we need to acquire a specific resource that returns
 * it's current state: String, Object, but not an active connection.
 *
 * @param {String}   resourceId
 * @param {String}   resourceType
 * @param {Function} createResource
 * @param {Function} next
 */
Node.prototype.acquireResource = function (resourceId, resourceType, opts, next) {
    var self = this;

    return this.hasResource(resourceId, resourceType)
        .catch(Errors.NotFound, function localResourceNotFound() {

            return Promise.fromNode(function acquireRemoteResource(callback) {

                var resourceKey = self.resourceKey(resourceType, resourceId);

                self._queue.push(resourceKey, callback, function lockAcquisition(err, workCompleted) {
                    if (err || !workCompleted) {
                        return;
                    }

                    // perform operation
                    self.askPeers(resourceId, resourceType)
                        .catch(Errors.NotFound, function noPeerHoldsResource() {
                            self.log.debug('Creating resource locally');
                            return self.createResource(resourceId, resourceType, opts.create)
                                .then(function () {
                                    return self.hasResource(resourceId, resourceType, opts.get);
                                });
                        })
                        .then(function (resourceData) {
                            self.log.debug('Result that had been passed:', resourceData);
                            return setImmediate(workCompleted, null, resourceData);
                        })
                        .catch(function (err) {
                            self.log.warn('Failed to complete work', err);
                            workCompleted(err);
                        });

                });

            });

        })
        .nodeify(next || _.noop);
};

/**
 * For some requests it is not possible to share state. For instance, there is an
 * open connection in the other node and we need to use it. For such methods resource
 * will be returning identifier (node-id) and we must redirect the query the node.
 *
 * Result of the operation will be returned back to this node and communicated back
 * to the client that requested it. Make sure not to transfer binary data as it's not
 * very efficient, make sure messaging is light-weight.
 *
 * If connection to node is lost before operation is reported to be successful a
 * retry of the whole operation chain will be issued.
 */
Node.prototype.redirectRequest = function (peerId, resourceId, resourceType, args, next) {
    var peer = this._peers[peerId];

    // peer that was reported to hold desired piece of information is down
    // do the operation ourselves
    if (!peer) {
        return Promise.reject(new Errors.Uninitialized()).nodeify(next || _.noop);
    }

        // peer exists, we can redirect query there
    return peer.request()
        .then(function (remote) {
            return Promise.fromNode(function (callback) {
                // if connection goes down mid-request
                function onRemoteDown() {
                    callback(new Errors.Uninitialized());
                }

                // if we are all-good
                function responded(err, response) {
                    peer.removeListener('down', onRemoteDown);
                    callback(err, response);
                }

                peer.once('down', onRemoteDown);
                remote.invoke(resourceId, resourceType, args, responded);
            });
        })
        .nodeify(next || _.noop);
};

/**
 * Wraps around response from acquireResource
 * @param {String}   resourceId
 * @param {String}   resourceType
 * @param {Object}   args
 * @param {Function} next
 */
Node.prototype.callResource = function (peerId, resourceId, resourceType, args, next) {
    if (this.server.id === peerId) {
        return this.invoke(resourceId, resourceType, args, next);
    } else {
        return this.redirectRequest(peerId, resourceId, resourceType, args, next);
    }
};

Node.prototype.askPeers = function (resourceId, resourceType, args, next) {
    var self = this;
    var peers = this._peers;
    // TODO: maintain keys index, save CPU
    var peerIds = _.keys(peers);

    self.log.debug('Querying peer ids:', peerIds);
    if (!peerIds.length) {
        return Promise.reject(new Errors.NotFound('Shared resource not found')).nodeify(next || _.noop);
    }

    return Promise
        .any(peerIds.map(function askPeer(peerId) {
            var peer = peers[peerId];
            return peer.request().then(function (remote) {
                    return Promise.fromNode(function (callback) {
                        remote.hasResource(resourceId, resourceType, args, callback);
                    });
                });
        }))
        .catch(Promise.AggregateError, function peerError(err) {
            var length = err.length;
            for (var i = 0; i < length; i++) {
                if (err[i].name !== Errors.NotFound.name) {
                    self.log.error('Unexpected errors in shared resources', err);
                }
            }

            return Promise.reject(new Errors.NotFound('Shared resource not found'));
        })
        .nodeify(next || _.noop);
};

/**
 * Invokes remotely callable functions
 * @param  {String}   resourceId
 * @param  {String}   resourceType
 * @param  {Array}    args
 * @param  {Function} next
 */
Node.prototype.invoke = function (resourceId, resourceType, args, next) {
    this.log.debug('Invoke for %s of %s called', resourceId, resourceType);

    var resourceHolder = this._resources[resourceType];
    if (!resourceHolder) {
        return Promise.reject(new Errors.Uninitialized('Resource not initialized')).nodeify(next || _.noop);
    }

    return resourceHolder.invoke(resourceId, args).nodeify(next || _.noop);
};

/**
 * Creates resource from a pre-defined set of resources
 * @param {String}   resourceId
 * @param {String}   resourceType
 * @param {Function} next
 */
Node.prototype.createResource = function (resourceId, resourceType, args, next) {
    var resource = this._resources[resourceType];
    if (!resource) {
        resource = this._resources[resourceType] = this._resource(resourceType, this);
    }

    return resource.create(resourceId, args).nodeify(next || _.noop);
};

/**
 * Returns resource value if it was found and is transferrable, otherwise
 * returns node id
 * @param {String}   resourceId
 * @param {String}   resourceType
 * @param {Function} next
 */
Node.prototype.hasResource = function (resourceId, resourceType, args, next) {
    var resource = this._resources[resourceType];
    if (resource) {
        return resource.get(resourceId, args).nodeify(next || _.noop);
    }

    this.log.debug('Resource %s not found on node %s', resourceType, this.server.id);
    return Promise.reject(new Errors.NotFound('Resource ' + resourceType + ' not found on node ' + this.server.id)).nodeify(next || _.noop);
};

/**
 * Sets up discoverd peer connections
 * @param {Object} configuration
 */
Node.prototype._setupPeers = function (configuration) {
    if (configuration === true) {
        this._options.peer = {
            ping: 500,
            timeout: 1500
        };
    }

    this._resources = {};
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
    var peers = this._peers;
    if (this.server && connectionString === this.server.id || peers[connectionString]) {
        return;
    }

    var peer = peers[connectionString] = new Peer(_.extend(parsed, this._options.peer));
    this._setupResourceScavenger(peer);
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

    // instanciate resource-constructor
    this._resource = resource(this._options.resources);

    this.server.attachPlugin('hasResource', this.hasResource);
    this.server.attachPlugin('invoke', this.invoke);
};

/**
 * Idea is that when a fellow peer dies, we want to scavenge all the resources
 * that it had. We will do it sequentially, so that all other nodes will be taking
 * it one by one
 */
Node.prototype._setupResourceScavenger = function (peer) {
    peer.on('down', this._scavengeResources);
};

/**
 * Peer died, lets acquire resources it had
 * @param {Object} peer
 */
Node.prototype._scavengeResources = function (peer) {
    this.log.warn('scavenge resources function not implemented. Peer %s down', peer.id);
};

/**
 * Closes node gracefully
 * @param  {Function} callback
 */
Node.prototype.close = function (callback) {
    var called = false;
    var callbacksToBeCalled = 1;
    var self = this;
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
        _.each(this._resources, function (resource) {
            callbacksToBeCalled++;
            resource.close(onClosed);
        });
        this.server.close(onClosed);
    }

    if (this._options.peer && this._peers) {
        pubsub.removeListener('messageBuffer', this._discoveredServer);
        _.each(this._peers, function (peer) {
            callbacksToBeCalled++;
            peer.removeListener('down', self._scavengeResources);
            peer.close(onClosed);
        });
        this._peers = {};
    }

    onClosed();
};

module.exports = Node;
