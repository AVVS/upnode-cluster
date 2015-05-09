'use strict';

var _ = require('lodash');
var inherits = require('util').inherits;
var LRU = require('lru-cache');
var Errors = require('node-common-errors');

function AbstractResource(opts) {
    this.cache = new LRU(opts.cache);
}

AbstractResource.prototype.create = function (resourceId, args, next) {
    return next(new Errors.Internal('create must be defined'));
};

AbstractResource.prototype.invoke = function (resourceId, args, next) {
    return next(new Errors.Internal('invoke must be defined'));
};

AbstractResource.prototype.get = function (resourceId, next) {
    return next(new Errors.Internal('get must be defined'));
};

var Resource = (function Resource(resources) {

    var ResourceConstructors = {};

    _.each(resources, function (resourceDefinition, resourceName) {

        function ResourceConstructor() {
            AbstractResource.call(this, resourceDefinition.opts);
        }
        inherits(ResourceConstructor, AbstractResource);

        ResourceConstructor.name = resourceName;

        _.extends(ResourceConstructor.prototype, resourceDefinition.prototype);

        // save reference
        ResourceConstructors[resourceName] = ResourceConstructor;
    });

    return function resourceConstructor(type) {
        var Constructor = ResourceConstructors[type];
        if (!Constructor) {
            throw new Errors.Common('Resource not found', 500);
        }

        return new Constructor();
    };

});

module.exports = Resource;
