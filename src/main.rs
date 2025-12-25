use clap::{Parser, Subcommand, ValueEnum};
use redb::{Database, ReadableDatabase, ReadableTable, TableHandle};
use postcard::from_bytes; // Keep from_bytes for deserializing Object::Eav in Commands::List
use std::path::PathBuf;

mod db;
use db::{
    EAV_TABLE, REPO_TABLE, REFS_TABLE, MST_ROOT_REF_NAME,
    Object, Db,
    hex_string, print_mst_recursive
};

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
    /// Displays the Merkle Search Tree from mst_root
    Ref {
        /// The name of the reference to display. Defaults to MST_ROOT_REF_NAME if not provided.
        #[arg(short, long)]
        ref_name: Option<String>,
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
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;
    let repo = Db::new(&db); // Instantiate Repo

    match &args.command {
        Commands::Write {
            entity,
            attribute,
            value,
        } => {
            // Call repo.write() instead of inline logic
            repo.write(entity, attribute, value)?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?}",
                entity, attribute, value, args.db_path
            );
        }
        Commands::Read { entity, attribute } => {
            // Call repo.read() instead of inline logic
            if let Some(read_value) = repo.read(entity, attribute)? {
                println!(
                    "Read from DB: ('{}', '{}', '{}')",
                    entity, attribute, read_value
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
                        let key_hex = hex_string(&key_guard.value());
                        match from_bytes::<Object>(value_guard.value().as_slice()) {
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
                        let val_hex = hex_string(value_guard.value());
                        println!("('{}', '{}')", ref_name, val_hex);
                    }
                }
            }
        }
        Commands::Ref { ref_name } => {
            let read_txn = db.begin_read()?;
            let repo_table = read_txn.open_table(REPO_TABLE)?;
            let refs_table = read_txn.open_table(REFS_TABLE)?;

            let target_ref_name = ref_name
                .as_deref()
                .unwrap_or(MST_ROOT_REF_NAME);

            println!("Displaying Merkle Search Tree from '{}':", target_ref_name);

            if let Some(mst_root_hash) = refs_table.get(target_ref_name)?.map(|guard| *guard.value()) {
                print_mst_recursive(&repo_table, Some(mst_root_hash))?;
            } else {
                println!("No MST root found for '{}'.", target_ref_name);
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
