/* global describe, it, after */

'use strict';

var expect = require('expect.js');
var _ = require('lodash');
var Redis = require('ioredis');
var async = require('neo-async');

describe('basic-connection', function () {

    var Node = require('../lib/node.js');
    var nodes = [];

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

    it('one of the nodes are closed', function (done) {
        nodes[1].close(done);
    });

    it('some time passes by', function (done) {
        setTimeout(done, 500);
    });

    it('make sure that connections are marked as down for node 1', function () {
        nodes.forEach(function (node, idx) {
            if (idx !== 1) {
                expect(node.server._clients).to.have.length(nodes.length - 2);
                expect(_.keys(node._peers)).to.have.length(nodes.length - 1);
                expect(node._peers[nodes[1].server.id]._connected).to.eql(false);
            } else {
                expect(node.server._clients).to.have.length(0);
                expect(_.keys(node._peers)).to.have.length(0);
            }
        });
    });

    it('reconnect node 1', function (done) {
        nodes[1] = new Node({
            redis: new Redis(),
            pubsubChannel: '{upnode-cluster}',
            cluster: 'upnode-cluster-ark',
            server: {
                port: 8001,
                host: 'localhost'
            },
            peer: true
        }, done);
    });

    it('time passes by', function (done) {
        setTimeout(done, 500);
    });

    it('make sure that all nodes are connected to each other', function () {
        nodes.forEach(function (node) {
            expect(node.server._clients).to.have.length(nodes.length - 1);
            expect(_.keys(node._peers)).to.have.length(nodes.length - 1);
        });
    });

    after(function (done) {
        async.each(nodes, function (q, next) {
            q.close(next);
        }, done);
    });

});
