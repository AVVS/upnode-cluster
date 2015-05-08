'use strict';

var _ = require('lodash');
var Logger = require('./logger.js');

var RPCWrapper = (function RPCWrapper(node) {

    var logger = Logger.child({ node_id: node.id, label: 'rpc' });

    function RPC(client, connection) {
        if (!(this instanceof RPC)) {
            return new RPC(client, connection);
        }

        _.bindAll(this);

        Object.defineProperty(this, '_connection', {
            value : connection,
            writable : false,
            enumerable: false
        });

        Object.defineProperty(this, '_client', {
            value : client,
            writable : false,
            enumerable: false
        });

        logger.info('created rpc server');
    }

    return RPC;

});

module.exports = RPCWrapper;
