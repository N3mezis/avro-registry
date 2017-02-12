    var lodash = require('lodash'),
    md5 = require('js-md5');

var AVRO_TYPES = {
    null: true,
    boolean: true,
    int: true,
    long: true,
    float: true,
    double: true,
    bytes: true,
    string: true,
    record: true,
    enum: true,
    array: true,
    map: true,
    fixed: true
};

var AVRO_KEYWORDS = {
    name: true,
    namespace: true,
    fields: true,
    type: true,
    default: true,
    order: true,
    symbols: true,
    items: true,
    size: true,
    values: true,
    protocol: true,
    request: true,
    response: true,
    errors: true
};

module.exports = function avroRegistry() {

    var registry = {
        schemas: {}
    };

    this.register = function (schema) {
        var name = schema.name,
            namespace = schema.namespace,
            path = namespace ? namespace + '.' + name : name,
            hashableSchema = getHashashableSchema(schema),
            hash = md5(JSON.stringify(hashableSchema)),
            schemaStore = lodash.get(registry, path);

        if (!schemaStore) {
            schemaStore = {
                versions: [],
                schemas: {},
                getSchema: function (version) {
                    version = typeof version === 'undefined' ?
                        schemaStore.versions.length - 1 : version;

                    return schemaStore.schemas[schemaStore.versions[version]];
                }
            };
            lodash.set(registry, path, schemaStore);
        }

        if (schemaStore.schemas[hash]) {
            return schemaStore.schemas[hash].snapshot;
        }

        schemaStore.schemas[hash] = {
            version: schemaStore.versions.length,
            schema: schema,
            snapshot: buildSnapshot(schema)
        };

        schemaStore.versions.push(hash);

        return schemaStore.schemas[hash];
    };

    this.get = function (path, version) {
        var schemaStore = lodash.get(registry, path);
        return schemaStore.getSchema(version);
    };

    function getHashashableSchema(schema) {
        if(typeof schema !== 'object') {
            return schema;
        }

        return Object.keys(schema).reduce(function(hashableSchema, key) {
            if(isAvroKeyword(key)) {
                hashableSchema[key] = typeof schema[key] === 'object' ?
                    getHashashableSchema(schema[key]) :
                    schema[key];
            }

            return hashableSchema;
        }, {});
    }

    function buildSnapshot(schema) {
        var dependencies = getTransitiveDependenciesForSnapshot(schema);

        return Object.keys(dependencies).reduce(function (snapshot, name) {
            var dependentSchemaStore = lodash.get(registry, name);

            lodash.set(snapshot, name, dependentSchemaStore.getSchema().schema);

            return snapshot;
        }, {});
    }

    function getTransitiveDependenciesForSnapshot(schema, dependencies, caller) {
        caller = caller || schema.name;

        return Object.keys(schema).reduce(function (dependencies, key) {
            if (typeof schema[key] === 'object') {
                return getTransitiveDependenciesForSnapshot(schema[key], dependencies, caller);
            }

            switch (key) {
                case 'type':
                case 'values':
                    var type = schema[key];
                    if (isCustomType(type)) {
                        if (!dependencies[type]) {
                            dependencies[type] = {};
                        }

                        //test for circular dependency -- currently possibly wrong - at least the error msg
                        if (dependencies[caller] && dependencies[caller][type]) {
                            throw 'Circular dependency found: ' + type + ' <-> ' + caller;
                        }

                        var customTypeSchema = lodash.get(registry, type);

                        if (!customTypeSchema) {
                            throw 'Schema ' + type + ' not registered!';
                        }

                        dependencies[type] = getTransitiveDependenciesForSnapshot(
                            customTypeSchema, dependencies, customTypeSchema.name
                        );
                    }
            }

            return dependencies
        }, dependencies || {});
    }

    function isAvroKeyword(key) {
        //Key might be array index -> check for number
        return AVRO_KEYWORDS[key] || !isNaN(Number(key));
    }

    function isCustomType(type) {
        return !AVRO_TYPES[type];
    }
};