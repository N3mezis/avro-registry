var lodash = require('lodash'),
    objectHash = require('object-hash'),
    avro = require('avsc');

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

var SETTINGS = {
    schemaEvolution: {
        default: 'strict',
        strict: 'strict',
        resolve: 'resolve'
    }
};

function setting(name, options) {
    var option = (options || {})[name] || SETTINGS[name].default;
    var invalid = !SETTINGS[name][option];

    if(invalid) {
        throw 'Setting \'' + name + '\' does not support option \'' + option + '\'!';
    }

    return option;
}

module.exports = function avroRegistry(options) {
    var registry = {
        schemas: {}
    }, settings = {
        schemaEvolution: setting('schemaEvolution', options)
    }, listeners = {};

    this.register = function (schema) {
        var name = schema.name,
            namespace = schema.namespace,
            path = namespace ? namespace + '.' + name : name,
            hash = this.hash(schema),
            schemaStore = lodash.get(registry, path);

        if (!schemaStore) {
            schemaStore = initSchemaStore(registry, path);
            dispatch('newSchema', schema);
        }

        if (schemaStore.schemas[hash]) {
            dispatch('oldSchema', schema, schemaStore.getSchema());
            return schemaStore.schemas[hash].snapshot;
        }

        schemaStore.schemas[hash] = {
            version: schemaStore.versions.length,
            schema: schema,
            snapshot: buildSnapshot(schema)
        };

        schemaStore.versions.push(hash);

        dispatch('updatedSchema', schema, schemaStore.getSchema(schemaStore.version - 1));

        if (settings.schemaEvolution === SETTINGS.schemaEvolution.resolve){
            var oldSchema = schemaStore.getSchema();

            if(oldSchema) {
                var oldType = avro.parse(oldSchema.schema, {registry: oldSchema.snapshot}),
                    newType = avro.parse(schema, {registry: schemaStore.schemas[hash].snapshot});

                try {
                    newType.createResolver(oldType);
                    schemaStore.majorVersions[schemaStore.majorVersions.length -1] = hash;
                    dispatch('updatedMajorSchema');
                } catch(e) {
                    schemaStore.majorVersions.push(hash);
                    dispatch('newMajorSchema');
                }
            } else {
                schemaStore.majorVersions.push(hash);
                dispatch('newMajorSchema');
            }
        }

        schemaStore.schemas[hash].majorVersion = schemaStore.majorVersions.length - 1;

        return schemaStore.schemas[hash];
    };

    this.hash = function(schema) {
        return  objectHash(getHashashableSchema(schema));
    };

    this.get = function (path, version) {
        var schemaStore = lodash.get(registry, path);
        return schemaStore.getSchema(version);
    };

    this.on = function(name, callback) {
        if(!listeners[name]) {
            listeners[name] = [];
        }

        listeners[name].push(callback);

        return function destroy() {
            return listeners[name].splice(listeners[name].indexOf(callback), 1);
        }
    };

    function dispatch(name) {
        if(!listeners.name) {
            return;
        }

        listeners[name].forEach(function(callback) {
            callback.apply(this, this.arguments.slice(1));
        });
    }

    function initSchemaStore(registry, path) {
        var schemaStore = {
            versions: [],
            majorVersions: [],
            schemas: {},
            getSchema: function (version) {
                var versions = settings.schemaEvolution === SETTINGS.schemaEvolution.resolve ?
                    schemaStore.majorVersions : schemaStore.versions;

                version = typeof version === 'undefined' ?
                    versions.length - 1 : version;
                version = version < 0 ?
                    versions.length - 1 + version : version;

                return schemaStore.schemas[versions[version]];
            }
        };

        lodash.set(registry, path, schemaStore);

        return schemaStore;
    }

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