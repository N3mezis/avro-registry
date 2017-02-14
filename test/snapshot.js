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
    name: 'test2',
    type: 'record',
    fields: [
        {
            name: 'test',
            type: 'test'
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

describe('snapshot', function() {
    var registry;

    beforeEach(function() {
        registry = new AvroRegistry({
            schemaEvolution: 'strict'
        })
    });

    it('should snapshot a dependent schema', function() {
        registry.register(schema1);

        assert.equal(registry.register(schema2).snapshot[schema1.name], schema1);
    });

    it('should snapshot the newest version of a dependent schema', function() {
        registry.register(schema1);
        registry.register(schema3);

        var result = registry.register(schema2);

        assert.equal(result.snapshot[schema1.name], schema3);
        assert.notEqual(result.snapshot[schema1.name], schema1);
    });
});