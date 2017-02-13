var assert = require('assert');
var AvroRegistry = require('../avroRegistry');

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
    fields: [
        {
            name: 'test',
            type: 'string'
        }
    ]
};

var schema3 = {
    name: 'test',
    type: 'record',
    fields: [
        {
            name: 'test',
            type: ['int', 'null']
        }
    ]
};

describe('avroRegistry', function() {
    describe('register(strict)', function() {
        var settings = {
            schemaEvolution: 'strict'
        };

        it('should register a new schema and set it as current', function() {

            var registry = new AvroRegistry(settings);

            assert.equal(registry.register(schema1).schema, schema1);
        });

        it('should not register a schema twice', function() {
            var registry = new AvroRegistry(settings);

            registry.register(schema1);
            registry.register(schema1);

            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.equal(registry.get(schema1.name, 1), undefined);
        });

        it('should update a registered schema', function() {
            var registry = new AvroRegistry(settings);

            registry.register(schema1);
            registry.register(schema2);

            assert.equal(registry.get(schema1.name, 1).schema, schema2);
            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.notEqual(registry.get(schema1.name, 1).schema, schema1);
        });
    });

    describe('register(resolve)', function() {
        var settings = {
            schemaEvolution: 'resolve'
        };

        it('should register a new schema and set it as current', function() {
            var registry = new AvroRegistry(settings);

            assert.equal(registry.register(schema1).schema, schema1);
        });

        it('should not register a schema twice', function() {
            var registry = new AvroRegistry(settings);

            registry.register(schema1);
            registry.register(schema1);

            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.equal(registry.get(schema1.name, 1), undefined);
        });

        it('should update a registered schema', function() {
            var registry = new AvroRegistry(settings);

            registry.register(schema1);
            registry.register(schema2);

            assert.equal(registry.get(schema1.name, 1).schema, schema2);
            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.notEqual(registry.get(schema1.name, 1).schema, schema1);
        });

        it('should not bump major version on a resolvable schema', function() {
            var eventFired = false;

            var registry = new AvroRegistry(settings);

            var registered1 = registry.register(schema1);
            var registered2 = registry.register(schema3);

            assert.equal(
                registered1.majorVersion,
                registered2.majorVersion
            );
        });
    });
});