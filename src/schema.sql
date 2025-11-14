-- [schema]
CREATE TABLE schema (
    name TEXT PRIMARY KEY NOT NULL,
    abstract INTEGER NOT NULL
);

CREATE TABLE schema_property (
    schema_name TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY(schema_name) REFERENCES schema(name)
);

CREATE TABLE schema_extend (
    schema_name TEXT NOT NULL,
    extends TEXT NOT NULL,
    FOREIGN KEY(schema_name) REFERENCES schema(name)
);

-- [entity]
CREATE TABLE entity (
    schema_name TEXT NOT NULL,    
    id TEXT NOT NULL,
    FOREIGN KEY(schema_name) REFERENCES schema(name)
);

-- [property]
CREATE TABLE entity_property (
    schema_name TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    property_name TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY(schema_name) REFERENCES schema(name)
    FOREIGN KEY(entity_id) REFERENCES entity(id)
    FOREIGN KEY(property_name) REFERENCES schema_property(name)
);