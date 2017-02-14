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

describe('register', function() {
    var registry;

    describe('strict', function() {
        beforeEach(function() {
            registry = new AvroRegistry({
                schemaEvolution: 'strict'
            })
        });


        it('should register a new schema and set it as current', function() {
            assert.equal(registry.register(schema1).schema, schema1);
        });

        it('should not register a schema twice', function() {
            registry.register(schema1);
            registry.register(schema1);

            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.equal(registry.get(schema1.name, 1), undefined);
        });

        it('should update a registered schema', function() {
            registry.register(schema1);
            registry.register(schema2);

            assert.equal(registry.get(schema1.name, 1).schema, schema2);
            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.notEqual(registry.get(schema1.name, 1).schema, schema1);
        });
    });

    describe('resolve', function() {
        beforeEach(function() {
            registry = new AvroRegistry({
                schemaEvolution: 'resolve'
            })
        });

        it('should register a new schema and set it as current', function() {
            assert.equal(registry.register(schema1).schema, schema1);
        });

        it('should not register a schema twice', function() {
            registry.register(schema1);
            registry.register(schema1);

            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.equal(registry.get(schema1.name, 1), undefined);
        });

        it('should update a registered schema', function() {
            registry.register(schema1);
            registry.register(schema2);

            assert.equal(registry.get(schema1.name, 1).schema, schema2);
            assert.equal(registry.get(schema1.name, 0).schema, schema1);
            assert.notEqual(registry.get(schema1.name, 1).schema, schema1);
        });

        it('should not bump major version on a resolvable schema', function() {
            var registered1 = registry.register(schema1);
            var registered2 = registry.register(schema3);

            assert.equal(
                registered1.majorVersion,
                registered2.majorVersion
            );

        });
    });
});