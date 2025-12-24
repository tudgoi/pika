use blake3::hash;
use clap::{Parser, Subcommand};
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
        /// The table to list (eav or repo)
        #[arg(default_value = "eav")]
        table_name: String,
    },
    /// Commits all records from eav and hash table to repo table
    Commit,
}

const EAV: TableDefinition<&str, &str> = TableDefinition::new("eav");
const REPO: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("repo");
const HASH: TableDefinition<&str, &[u8; 32]> = TableDefinition::new("hash");

#[derive(Serialize, Deserialize, Debug)]
struct Record<'a>(&'a str, &'a str);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write { key, value } => {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(EAV)?;
                table.insert(key.as_str(), value.as_str())?;

                let record = Record(key.as_str(), value.as_str());
                let encoded = to_stdvec(&record)?;
                let hash = hash(&encoded);

                let mut hash_table = write_txn.open_table(HASH)?;
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
            let table = read_txn.open_table(EAV)?;
            if let Some(read_value) = table.get(key.as_str())? {
                println!("Read from DB: ('{}', '{}')", key, read_value.value());
            } else {
                println!("Key '{}' not found in database.", key);
            }
        }
        Commands::List { table_name } => {
            let read_txn = db.begin_read()?;
            if table_name == "eav" {
                let table = read_txn.open_table(EAV)?;
                println!("Listing all items in 'eav':");
                for result in table.iter()? {
                    let (key, value) = result?;
                    println!("('{}', '{}')", key.value(), value.value());
                }
            } else if table_name == "repo" {
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
            } else if table_name == "hash" {
                let table = read_txn.open_table(HASH)?;
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
            } else {
                eprintln!(
                    "Error: Unknown table '{}'. Available tables: 'eav', 'repo', 'hash'",
                    table_name
                );
            }
        }
        Commands::Commit => {
            let write_txn = db.begin_write()?;
            {
                let eav_table = write_txn.open_table(EAV)?;
                let hash_table = write_txn.open_table(HASH)?;
                let mut repo_table = write_txn.open_table(REPO)?;

                // Collect entries first to avoid holding borrow on eav_table while writing to repo_table
                // Although redb allows multiple tables open, iterating one while writing another in same txn is generally fine
                // but we need to be careful with lifetimes if we hold guards.
                // However, the error was about access methods.

                // Because we are iterating `eav_table` and need to read `hash_table` and write `repo_table`,
                // and `redb` transaction ownership rules can be tricky with multiple open tables if not scoped perfectly,
                // let's just make sure we are clean.

                // Actually, the previous code structure was fine regarding lifetimes if methods were correct.

                for result in eav_table.iter()? {
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
    }

    Ok(())
}
