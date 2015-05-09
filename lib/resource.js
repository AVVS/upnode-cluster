'use strict';

var _ = require('lodash');
var inherits = require('util').inherits;
var LRU = require('lru-cache');
var Errors = require('node-common-errors');

function AbstractResource(opts) {
    this._opts = opts.cache;
    this._cache = new LRU(opts.cache);
}

AbstractResource.prototype.cache = function (resourceId, resourceData, next) {
    var node = this.node;
    var id = node.server.id;
    var redis = node._redis;
    var tasksSet = node.resourceKey(node._resourceChannel, id, this.name);
    var tasksDetails = node.resourceKey(tasksSet, resourceId);

    // cache resource locally
    this._cache.set(resourceId, resourceData);

    // upload notification that we own this resource to redis
    return redis
            .pipeline()
            .sadd(tasksSet, resourceId)
            .hmset(tasksDetails, this.toJSON(resourceData))
            .expire(tasksDetails, this._opts.ttl)
            .exec(next);
};

AbstractResource.prototype.create = function (resourceId, args, next) {
    return next(new Errors.Internal('create must be defined'));
};

AbstractResource.prototype.invoke = function (resourceId, args, next) {
    return next(new Errors.Internal('invoke must be defined'));
};

AbstractResource.prototype.restore = function (resourceId, resourceData, next) {
    return next(new Error(resourceId + ' does not support restore feature'));
};

AbstractResource.prototype.toJSON = function (resourceId) {
    throw new Error('resource ' + resourceId + 'must implement toJSON() interface in order to be saved and exported');
};

AbstractResource.prototype.get = function (resourceId, next) {
    return next(new Errors.Internal('get must be defined'));
};

AbstractResource.prototype.close = function (next) {
    // please, specify something that can cleanup the resource gracefully on exit
    next();
};

var Resource = (function Resource(resources) {

    var ResourceConstructors = {};

    _.each(resources, function (resourceDefinition, resourceName) {

        function ResourceConstructor(node) {
            AbstractResource.call(this, resourceDefinition.opts);
            this.node = node;
        }
        inherits(ResourceConstructor, AbstractResource);

        ResourceConstructor.name = resourceName;

        _.extends(ResourceConstructor.prototype, resourceDefinition.prototype);

        // save reference
        ResourceConstructors[resourceName] = ResourceConstructor;
    });

    return function resourceConstructor(type, node) {
        var Constructor = ResourceConstructors[type];
        if (!Constructor) {
            throw new Errors.Common('Resource not found', 500);
        }

        return new Constructor(node);
    };

});

module.exports = Resource;
