-- [schema]
CREATE TABLE schema (
    name TEXT NOT NULL,
    abstract INTEGER NOT NULL
);

CREATE TABLE schema_property (
    schema_name TEXT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL
);