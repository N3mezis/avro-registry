var assert = require('assert');
var AvroRegistry = require('../avroRegistry');

describe('hash', function () {
    var registry;
    beforeEach(function () {
        registry = new AvroRegistry()
    });

    it('should return the same hash for two equal schemas', function () {
        var schema = {
            name: 'test',
            type: 'record',
            fields: [
                {
                    name: 'test',
                    type: 'int'
                }
            ]
        };

        assert.equal(registry.hash(schema), registry.hash(schema));
    });

    it('should return the same hash for two schemas with different documentation', function () {
        var schema1 = {
            name: 'test',
            type: 'record',
            fields: [
                {
                    name: 'test',
                    type: 'int'
                }
            ]
        };

        var schema2 = {
            name: 'test',
            type: 'record',
            doc: 'this is a documentation',
            fields: [
                {
                    name: 'test',
                    type: 'int'
                }
            ]
        };

        assert.equal(registry.hash(schema1), registry.hash(schema2));
    });

    it('should return the same hash for two equal schemas with different ordering', function () {
        var schema1 = {
            name: 'test',
            type: 'record',
            fields: [
                {
                    name: 'test',
                    type: 'int'
                }
            ]
        };

        var schema2 = {
            type: 'record',
            name: 'test',
            fields: [
                {
                    type: 'int',
                    name: 'test'

                }
            ]
        };

        assert.equal(registry.hash(schema1), registry.hash(schema2));
    });

    it('should return different hashes for two different schemas', function () {
        var schema1 = {
            name: 'test',
            type: 'record',
            fields: [
                {
                    name: 'test',
                    type: 'string'
                }
            ]
        };

        var schema2 = {
            type: 'record',
            name: 'test',
            fields: [
                {
                    type: 'string',
                    name: 'test2'

                }
            ]
        };

        assert.notEqual(registry.hash(schema1), registry.hash(schema2));
    });
});