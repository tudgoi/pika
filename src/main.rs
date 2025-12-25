use blake3::hash;
use clap::{Parser, Subcommand, ValueEnum};
use postcard::{from_bytes, to_stdvec};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
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
        /// The key to write
        key: String,

        /// The value to write
        value: String,
    },
    /// Reads a value from the database given a key
    Read {
        /// The key to read
        key: String,
    },
    /// Lists all key-value pairs in the database
    List {
        /// The table to list
        #[arg(default_value = "kv")]
        table_name: Tables,
    },
    /// Commits all records from kv and hash table to repo table
    Commit,
    /// Garbage collect unreferenced records from repo
    Gc,
}

#[derive(Debug, Clone, ValueEnum)]
enum Tables {
    Kv,
    Repo,
    Refs,
}

const KV: TableDefinition<&str, &str> = TableDefinition::new("kv");
const REPO: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("repo");
const REFS: TableDefinition<&str, &[u8; 32]> = TableDefinition::new("refs");

#[derive(Serialize, Deserialize, Debug)]
struct Record<'a>(&'a str, &'a str);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write { key, value } => {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(KV)?;
                table.insert(key.as_str(), value.as_str())?;

                let record = Record(key.as_str(), value.as_str());
                let encoded = to_stdvec(&record)?;
                let hash = hash(&encoded);

                let mut hash_table = write_txn.open_table(REFS)?;
                hash_table.insert(key.as_str(), hash.as_bytes())?;
            }
            write_txn.commit()?;
            println!(
                "Successfully wrote key-value pair ('{}', '{}') to database at: {:?}",
                key, value, args.db_path
            );
        }
        Commands::Read { key } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(KV)?;
            if let Some(read_value) = table.get(key.as_str())? {
                println!("Read from DB: ('{}', '{}')", key, read_value.value());
            } else {
                println!("Key '{}' not found in database.", key);
            }
        }
        Commands::List { table_name } => {
            let read_txn = db.begin_read()?;
            match table_name {
                Tables::Kv => {
                    let table = read_txn.open_table(KV)?;
                    println!("Listing all items in 'kv':");
                    for result in table.iter()? {
                        let (key, value) = result?;
                        println!("('{}', '{}')", key.value(), value.value());
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
                        let record: Record =
                            from_bytes(value.value()).unwrap_or(Record("error", "error"));
                        println!("('{}', '{:?}')", key_hex, record);
                    }
                }
                Tables::Refs => {
                    let table = read_txn.open_table(REFS)?;
                    println!("Listing all items in 'hash':");
                    for result in table.iter()? {
                        let (key, value) = result?;
                        let val_hex = value
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        println!("('{}', '{}')", key.value(), val_hex);
                    }
                }
            }
        }
        Commands::Commit => {
            let write_txn = db.begin_write()?;
            {
                let kv_table = write_txn.open_table(KV)?;
                let hash_table = write_txn.open_table(REFS)?;
                let mut repo_table = write_txn.open_table(REPO)?;

                for result in kv_table.iter()? {
                    let (key_guard, value_guard) = result?;
                    let key = key_guard.value();
                    let value = value_guard.value();

                    let record = Record(key, value);
                    let encoded = to_stdvec(&record)?;

                    // We expect the hash to be in the HASH table as per Write command logic
                    if let Some(hash_guard) = hash_table.get(key)? {
                        let hash_val = hash_guard.value();

                        if repo_table.get(hash_val)?.is_none() {
                            repo_table.insert(hash_val, encoded.as_slice())?;
                        }
                    } else {
                        eprintln!(
                            "Warning: No hash found for key '{}' in HASH table. Skipping repo update.",
                            key
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
                        let repo_key = record.0;
                        if let Some(hash_value) = hash_table.get(repo_key)? {
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
