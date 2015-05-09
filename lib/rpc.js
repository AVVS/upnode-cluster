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

        Object.defineProperty(this, '_log', {
            value : logger,
            writable : false,
            enumerable: false
        });

        this._log.info('created rpc server');
    }

    /**
     * Attaches rpc-enabled function
     * @param {String}   name
     * @param {Function} func
     */
    RPC.attachPlugin = function (name, func) {
        RPC.prototype[name] = func;
    };

    /**
     * Detached rpc-enabled function
     * @param {String} name
     */
    RPC.detachPlugin = function (name) {
        if (typeof RPC.prototype[name] === 'function') {
            delete RPC.prototype[name];
        }
    };

    return RPC;
});

module.exports = RPCWrapper;
