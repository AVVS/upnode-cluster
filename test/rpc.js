/* global describe, it, after */

'use strict';

var expect = require('expect.js');
var _ = require('lodash');
var Redis = require('ioredis');
var async = require('neo-async');
var Errors = require('node-common-errors');

describe('rpc', function () {

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

    it('is able to ask peers', function (done) {
        nodes[0].askPeers('email@ark.com', 'credentials-email', [], function (err, response) {
            expect(err).to.be.an(Errors.NotFound);
            expect(response).to.be(undefined);
            done();
        });
    });

    it('is able to create resource if it is not found', function (done) {
        var node = nodes[0];

        node.acquireResource('email@ark.com', 'credentials-email', [], function (err, response) {
            expect(err).to.be.an(Errors.Common);
            expect(response).to.be(undefined);
            done();
        });
    });


    after(function (done) {
        async.each(nodes, function (q, next) {
            q.close(next);
        }, done);
    });

});
