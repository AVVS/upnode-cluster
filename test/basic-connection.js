/* global describe, it */

'use strict';

var expect = require('expect.js');
var _ = require('lodash');
var Redis = require('ioredis');

describe('basic-connection', function () {

    var Server = require('../lib/server.js');
    var Peer = require('../lib/peer.js');
    var Node = require('../lib/node.js');

    var serverOpts = {
        port: 7777,
        host: 'localhost'
    };

    it('is able to bootstrap server', function (done) {
        this.server = new Server(_.extend({}, serverOpts), done);
    });

    it('is able to connect to server', function (done) {
        this.peer = new Peer(_.extend({}, serverOpts), done);
    });

    it('is able to close server', function (done) {
        this.server.close(done);
    });

    it('is able to close peer connection', function (done) {
        this.peer.close(done);
    });

    it('able to setup server node', function (done) {
        this.server = new Node({
            server: serverOpts,
            redis: new Redis(),
            pubsubChannel: '{upnode-cluster}',
            cluster: 'upnode-cluster-ark'
        }, done);
    });

    it('able to setup peer node', function (done) {
        this.peer = new Node({
            redis: new Redis(),
            pubsubChannel: '{upnode-cluster}',
            cluster: 'upnode-cluster-ark',
            peer: true
        }, done);
    });

    it('some time passes by...', function (done) {
        setTimeout(done, 500);
    });

    it('peer must be connected to server', function () {
        expect(this.server.server._clients).to.have.length(1);
        expect(_.keys(this.peer._peers)).to.have.length(1);
        expect(this.peer._peers['localhost:7777']).to.be.an('object');
    });

    it('launch multi-node', function (done) {
        this.node = new Node({
            redis: new Redis(),
            pubsubChannel: '{upnode-cluster}',
            cluster: 'upnode-cluster-ark',
            peer: true,
            server: {
                port: 7778,
                host: 'localhost'
            }
        }, done);
    });

    it('some time passes by', function (done) {
        setTimeout(done, 500);
    });

    it('expect multi-node to be connected to other servers', function () {
        expect(_.keys(this.node._peers)).to.have.length(1);
        expect(this.server.server._clients).to.have.length(2);
        expect(this.node.server._clients).to.have.length(1);
        expect(_.keys(this.peer._peers)).to.have.length(2);
    });

});
