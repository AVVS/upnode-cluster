'use strict';

var _ = require('lodash');
var inherits = require('util').inherits;
var LRU = require('lru-cache');
var Errors = require('node-common-errors');

function AbstractResource(opts) {
    this._opts = opts.cache;
    this._cache = new LRU(opts.cache);
}

AbstractResource.prototype.cache = function (resourceId, resource, next) {
    var node = this.node;
    var id = node.server.id;
    var redis = node._redis;
    var tasksSet = node.resourceKey(node._resourceChannel, id, this.name);
    var tasksDetails = node.resourceKey(tasksSet, resourceId);

    // cache resource locally
    this._cache.set(resourceId, resource);

    // upload notification that we own this resource to redis
    return redis
            .pipeline()
            .sadd(tasksSet, resourceId)
            .hmset(tasksDetails, this.serialize(resource))
            .pexpire(tasksDetails, this._opts.maxAge)
            .exec(next);
};

AbstractResource.prototype.create = function (resourceId, args, next) {
    return next(new Errors.Internal('create must be defined'));
};

AbstractResource.prototype.invoke = function (resourceId, args, next) {
    return next(new Errors.Internal('invoke must be defined'));
};

AbstractResource.prototype.deserialize = function (resourceId, resourceData, next) {
    return next(new Error.Internal(resourceId + ' does not support restore feature'));
};

AbstractResource.prototype.serialize = function (resourceId) {
    throw new Error.Internal('resource ' + resourceId + 'must implement toJSON() interface in order to be saved and exported');
};

AbstractResource.prototype.update = function (resourceId, args, next) {
    return next(new Errors.Internal('Resource does not support update feature'));
};

AbstractResource.prototype.get = function (resourceId, args, next) {
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
            this.name = resourceName;
        }
        inherits(ResourceConstructor, AbstractResource);

        ResourceConstructor.name = resourceName;

        _.extend(ResourceConstructor.prototype, resourceDefinition.prototype);

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
