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
    /// Commits all records from kv and hash table to repo table
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

const EAV: TableDefinition<(&str, &str), &str> = TableDefinition::new("eav");
const REPO: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("repo");
const REFS: TableDefinition<(&str, &str), &[u8; 32]> = TableDefinition::new("refs");

#[derive(Serialize, Deserialize, Debug)]
struct Record<'a>(&'a str, &'a str, &'a str);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write { entity, attribute, value } => {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(EAV)?;
                table.insert((entity.as_str(), attribute.as_str()), value.as_str())?;

                let record = Record(entity.as_str(), attribute.as_str(), value.as_str());
                let encoded = to_stdvec(&record)?;
                let hash = hash(&encoded);

                let mut hash_table = write_txn.open_table(REFS)?;
                hash_table.insert((entity.as_str(), attribute.as_str()), hash.as_bytes())?;
            }
            write_txn.commit()?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?}",
                entity, attribute, value, args.db_path
            );
        }
        Commands::Read { entity, attribute } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(EAV)?;
            if let Some(read_value) = table.get(&(entity.as_str(), attribute.as_str()))? {
                println!(
                    "Read from DB: ('{}', '{}', '{}')",
                    entity,
                    attribute,
                    read_value.value()
                );
            } else {
                println!("EAV triple ('{}', '{}') not found in database.", entity, attribute);
            }
        }
        Commands::List { table_name } => {
            let read_txn = db.begin_read()?;
            match table_name {
                Tables::Eav => {
                    let table = read_txn.open_table(EAV)?;
                    println!("Listing all items in {}:", EAV.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let (entity, attribute) = key_guard.value();
                        println!("('{}', '{}', '{}')", entity, attribute, value_guard.value());
                    }
                }
                Tables::Repo => {
                    let table = read_txn.open_table(REPO)?;
                    println!("Listing all items in 'repo':");
                    for result in table.iter()? {
                        let (key, value) = result?;
                        let key_hex = key
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        // TODO avoid this hack for displaying error
                        let record: Record =
                            from_bytes(value.value()).unwrap_or(Record("error", "error", "error"));
                        println!("('{}', '{:?}')", key_hex, record);
                    }
                }
                Tables::Refs => {
                    let table = read_txn.open_table(REFS)?;
                    println!("Listing all items in 'hash':");
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let (entity, attribute) = key_guard.value();
                        let val_hex = value_guard
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        println!("('{}', '{}', '{}')", entity, attribute, val_hex);
                    }
                }
            }
        }
        Commands::Commit => {
            let write_txn = db.begin_write()?;
            {
                let kv_table = write_txn.open_table(EAV)?;
                let hash_table = write_txn.open_table(REFS)?;
                let mut repo_table = write_txn.open_table(REPO)?;

                for result in kv_table.iter()? {
                    let (key_guard, value_guard) = result?;
                    let (entity, attribute) = key_guard.value();
                    let value = value_guard.value();

                    let record = Record(entity, attribute, value);
                    let encoded = to_stdvec(&record)?;

                    // We expect the hash to be in the HASH table as per Write command logic
                    if let Some(hash_guard) = hash_table.get(&(entity, attribute))? {
                        let hash_val = hash_guard.value();

                        if repo_table.get(hash_val)?.is_none() {
                            repo_table.insert(hash_val, encoded.as_slice())?;
                        }
                    } else {
                        eprintln!(
                            "Warning: No hash found for EAV ('{}', '{}') in HASH table. Skipping repo update.",
                            entity, attribute
                        );
                    }
                }
            }
            write_txn.commit()?;
            println!("Successfully committed records to repo.");
        }
        Commands::Gc => {
            let write_txn = db.begin_write()?;
            let mut to_delete = Vec::new();
            {
                let repo_table = write_txn.open_table(REPO)?;
                let hash_table = write_txn.open_table(REFS)?;

                for repo_result in repo_table.iter()? {
                    let (repo_key_result, repo_value_result) = repo_result?;
                    let repo_hash = repo_key_result.value();

                    if let Ok(record) = from_bytes::<Record>(repo_value_result.value()) {
                        let Record(entity, attribute, _) = record;
                        if let Some(hash_value) = hash_table.get(&(entity, attribute))? {
                            if hash_value.value() != repo_hash {
                                to_delete.push(*repo_hash);
                            }
                        } else {
                            to_delete.push(*repo_hash);
                        }
                    } else {
                        eprintln!(
                            "Warning: Failed to deserialize record for hash {:?}. Skipping.",
                            repo_hash
                        );
                    }
                }
            }
            {
                let mut repo_table = write_txn.open_table(REPO)?;
                for hash in to_delete {
                    repo_table.remove(&hash)?;
                    let hash_hex = hash
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    println!("Garbage collected: {}", hash_hex);
                }
            }
            write_txn.commit()?;
            println!("Garbage collection completed.");
        }
    }

    Ok(())
}
