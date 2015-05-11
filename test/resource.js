/* global describe, it, after */

'use strict';

var expect = require('expect.js');
var Redis = require('ioredis');
var _ = require('lodash');
var async = require('neo-async');
var Errors = require('node-common-errors');
var Promise = require('bluebird');

var TestResource = {

    opts: {

        cache: {

            dispose: function (key, resource) {
                resource.dispose();
            },

            maxAge: 30000 // 30 sec

        }

    },

    prototype: {

        create: function (resourceId, args, next) {
            var resource = this._cache.get(resourceId);

            var promise;
            if (!resource) {
                resource = {
                    id: resourceId,
                    args: {
                        increment: 2
                    },
                    intervalsTicked: 0,
                    interval: setInterval(function () {
                        resource.intervalsTicked += args.increment;
                    }, 1000),
                    dispose: function () {
                        clearInterval(this.interval);
                    }
                };

                this._cache.set(resourceId, resource);
                promise = this.get(resourceId);
            } else {
                promise = this.update(resource, args);
            }

            return promise.nodeify(next || _.noop);
        },

        update: function (resource, args) {
            resource.args = args;
            return Promise.resolve(resource);
        },

        invoke: function (resourceId, args, next) {
            var resource = this._cache.get(resourceId);

            if (!resource) {
                return Promise.reject(new Errors.NotFound(resourceId)).nodeify(next);
            }

            return Promise.resolve(resource).then(function (resource) {
                console.log('beep boop %s. Passed args:', resource.id, args);
                return resource;
            }).nodeify(next);
        },

        get: function (resourceId, args, next) {
            var resource = this._cache.get(resourceId);
            if (!resource) {
                return Promise.reject(new Errors.NotFound()).nodeify(next || _.noop);
            }

            return Promise.resolve(resource.intervalsTicked).nodeify(next || _.noop);
        },

        close: function (resourceId, next) {
            this._cache.del(resourceId);
            return Promise.resolve(true).nodeify(next);
        },

        deserialize: function (resourceId, resourceData, next) {
            return this.create(resourceId, resourceData).nodeify(next);
        },

        serialize: function (resourceId) {
            var resource = this._cache.peek(resourceId);
            if (!resource) {
                return false;
            }

            return resource.args;
        }

    }

};

describe('resource', function () {

    var Node = require('../lib/node.js');
    var nodes = [];
    var resources = {};

    resources.TestResource = TestResource;

    it('start 3 interconnected nodes', function (done) {

        async.times(3, function (n, next) {

            nodes.push(new Node({
                redis: new Redis(),
                pubsubChannel: '{upnode-cluster}',
                cluster: 'upnode-cluster-ark',
                server: {
                    port: 8000 + n,
                    host: 'localhost'
                },
                resources: resources,
                peer: true
            }, next));

        }, done);

    });

    it('some time passes by', function (done) {
        setTimeout(done, 500);
    });

    it('make sure that all nodes are connected to each other', function () {
        nodes.forEach(function (node) {
            expect(node.server._clients).to.have.length(nodes.length - 1);
            expect(_.keys(node._peers)).to.have.length(nodes.length - 1);
        });
    });

    it('is able to create resource', function (done) {

        var node = nodes[0];
        var args = { vitaly: 'aminev', ark: '.com' };

        node
            .acquireResource('test', 'TestResource', args)
            .then(function (resource) {
                expect(resource).to.eql(0);
            })
            .nodeify(done);
    });

    after(function (done) {
        async.each(nodes, function (q, next) {
            q.close(next);
        }, done);
    });

});
