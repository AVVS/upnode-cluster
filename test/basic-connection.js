/* global describe, it */

'use strict';

var expect = require('expect.js');
var _ = require('lodash');

describe('basic-connection', function () {

    var Server = require('../lib/server.js');
    var Peer = require('../lib/peer.js');

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

});
