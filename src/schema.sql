-- [schema]
CREATE TABLE schema (
    name TEXT NOT NULL,
    abstract INTEGER NOT NULL,
    PRIMARY KEY(name)
);
CREATE TABLE schema_property (
    schema_name TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY(schema_name, name) FOREIGN KEY(schema_name) REFERENCES schema(name)
);
CREATE TABLE schema_extend (
    schema_name TEXT NOT NULL,
    extends TEXT NOT NULL,
    PRIMARY KEY(schema_name) FOREIGN KEY(schema_name) REFERENCES schema(name)
);
-- [entity]
CREATE TABLE entity (
    schema_name TEXT NOT NULL,
    id TEXT NOT NULL,
    PRIMARY KEY(schema_name, id) FOREIGN KEY(schema_name) REFERENCES schema(name)
);
-- [property]
CREATE TABLE entity_property (
    entity_schema_name TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    property_schema_name TEXT NOT NULL,
    property_name TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(
        entity_schema_name,
        entity_id,
        property_schema_name,
        property_name
    ) FOREIGN KEY(entity_schema_name, entity_id) REFERENCES entity(schema_name, id) FOREIGN KEY(property_schema_name, property_name) REFERENCES schema_property(schema_name, name)
);
-- [source]
CREATE TABLE source (
    id INTEGER,
    url TEXT NOT NULL,
    crawl_date TEXT,
    force_crawl BOOLEAN,
    PRIMARY KEY(id) UNIQUE(url)
);
-- [document]
CREATE TABLE document (
    id TEXT,
    source_id INTEGER NOT NULL,
    retrieved_date TEXT NOT NULL,
    etag TEXT,
    title TEXT,
    content TEXT NOT NULL,
    PRIMARY KEY(id) FOREIGN KEY(source_id) REFERENCES source(id)
);