use blake3::hash;
use clap::{Parser, Subcommand, ValueEnum};
use postcard::{from_bytes, to_stdvec};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the redb database file
    db_path: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Writes a key-value pair to the database
    Write {
        /// The entity
        entity: String,
        /// The attribute
        attribute: String,
        /// The value to write
        value: String,
    },
    /// Reads a value from the database given a key
    Read {
        /// The entity to read
        entity: String,
        /// The attribute to read
        attribute: String,
    },
    /// Lists all key-value pairs in the database
    List {
        /// The table to list
        #[arg(value_enum, default_value_t = Tables::Eav)]
        table_name: Tables,
    },
    /// Commits all records into repo
    Commit,
    /// Garbage collect unreferenced records from repo
    Gc,
}

#[derive(Debug, Clone, ValueEnum)]
enum Tables {
    Eav,
    Repo,
    Refs,
}

type Entity = str;
type Attribute = str;
type EavValue = str;
type RefName = str;
type Hash = [u8; 32];
type Blob = [u8];

const EAV_TABLE: TableDefinition<(&Entity, &Attribute), &EavValue> = TableDefinition::new("eav");
const REPO_TABLE: TableDefinition<&Hash, &Blob> = TableDefinition::new("repo");
const REFS_TABLE: TableDefinition<&RefName, &Hash> = TableDefinition::new("refs");

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Object<'a> {
    Eav(&'a str, &'a str, &'a str),
    RefList(Vec<Hash>),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write {
            entity,
            attribute,
            value,
        } => {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(EAV_TABLE)?;
                table.insert((entity.as_str(), attribute.as_str()), value.as_str())?;

                let record = Object::Eav(entity, attribute, value);
                let encoded = to_stdvec(&record)?;
                let hash = hash(&encoded);
                let hash_value = hash.as_bytes();

                let mut repo_table = write_txn.open_table(REPO_TABLE)?;
                if repo_table.get(&hash_value)?.is_none() {
                    repo_table.insert(hash_value, encoded.as_slice())?;
                }

                let mut refs_table = write_txn.open_table(REFS_TABLE)?;
                refs_table.insert("recent", hash_value)?;
            }
            write_txn.commit()?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?}",
                entity, attribute, value, args.db_path
            );
        }
        Commands::Read { entity, attribute } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(EAV_TABLE)?;
            if let Some(read_value) = table.get(&(entity.as_str(), attribute.as_str()))? {
                println!(
                    "Read from DB: ('{}', '{}', '{}')",
                    entity,
                    attribute,
                    read_value.value()
                );
            } else {
                println!(
                    "EAV triple ('{}', '{}') not found in database.",
                    entity, attribute
                );
            }
        }
        Commands::List { table_name } => {
            let read_txn = db.begin_read()?;
            match table_name {
                Tables::Eav => {
                    let table = read_txn.open_table(EAV_TABLE)?;
                    println!("Listing all items in {}:", EAV_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let (entity, attribute) = key_guard.value();
                        println!("('{}', '{}', '{}')", entity, attribute, value_guard.value());
                    }
                }
                Tables::Repo => {
                    let table = read_txn.open_table(REPO_TABLE)?;
                    println!("Listing all items in {}:", REPO_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let key_hex = key_guard
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        match from_bytes::<Object>(value_guard.value()) {
                            Ok(obj) => println!("('{}', '{:?}')", key_hex, obj),
                            Err(e) => println!("Could not deserialize for {}: {}", key_hex, e),
                        }
                    }
                }
                Tables::Refs => {
                    let table = read_txn.open_table(REFS_TABLE)?;
                    println!("Listing all items in {}:", REFS_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let ref_name = key_guard.value();
                        let val_hex = value_guard
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        println!("('{}', '{}')", ref_name, val_hex);
                    }
                }
            }
        }
        Commands::Commit => {
            println!("Not yet implemented");
        }
        Commands::Gc => {
            println!("Not yet implemented");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialization_deserialization() {
        let original_record = Object::Eav("entity1", "attribute1", "value1");

        let encoded = to_stdvec(&original_record).expect("Failed to serialize record");
        let decoded_record: Object = from_bytes(&encoded).expect("Failed to deserialize record");

        assert_eq!(original_record, decoded_record);
    }
}
