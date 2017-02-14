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

describe('events', function () {
    var registry;

    beforeEach(function() {
        registry = new AvroRegistry()
    });

    it('should emit newSchema for a first seen schema', function () {
        var eventFired = false;

        registry.on('newSchema', function () {
            eventFired = true;
        });

        assert.equal(eventFired, false);
        registry.register(schema1);
        assert.equal(eventFired, true);
    });
});