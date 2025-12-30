mod db;
mod mst;
mod pt;
mod serve;
mod sync;

use crate::mst::hex_string;
use anyhow::{Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use redb::{ReadableDatabase, ReadableTable, TableHandle};
use std::path::PathBuf;

use db::{Db, EAV_TABLE, Engine, REFS_TABLE, REPO_TABLE, ROOT_REF_NAME};

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
    /// Initialize the database
    Init {
        /// Storage engine to use
        #[arg(short, long, value_enum, default_value_t = Engine::Mst)]
        engine: Engine,
    },
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
    /// Displays a specific key value from the ref or the entire ref as a tree
    Ref {
        /// The ref name which needs to be displayed. If key is not specified, the entire ref is displayed as a tree.
        ref_name: Option<String>,

        /// The name of the entity whose attribute value needs to be displayed
        entity: Option<String>,

        /// The name of the attribute whose value needs to be displayed
        attribute: Option<String>,
    },
    /// Commits all records into repo
    Commit,
    /// Garbage collect unreferenced records from repo
    Gc,
    /// Displays overhead stats
    Stat,
    /// Serves iroh endpoint for syncing
    Serve,
    /// Sync DB with given remote endpoint
    Sync {
        endpoint: iroh::EndpointId,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum Tables {
    Eav,
    Repo,
    Refs,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Commands::Init { engine } = args.command {
        Db::init(&args.db_path, engine)?;
        println!(
            "Initialized database at {:?} with engine {:?}",
            args.db_path, engine
        );
        return Ok(());
    }

    if matches!(args.command, Commands::Stat) && !args.db_path.exists() {
        bail!("Database file does not exist: {:?}", args.db_path)
    }

    let db = Db::open(&args.db_path)?;

    match &args.command {
        Commands::Init { .. } => unreachable!(),
        Commands::Write {
            entity,
            attribute,
            value,
        } => {
            // Call repo.write() instead of inline logic
            db.write(entity, attribute, value)?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?} using {:?}",
                entity, attribute, value, args.db_path, db.engine
            );
        }
        Commands::Read { entity, attribute } => {
            // Call repo.read() instead of inline logic
            if let Some(read_value) = db.read(entity, attribute)? {
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
            let read_txn = db.redb.begin_read()?;
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
                        // Try to deserialize as MstNode first, then PtNode, or just print hex
                        // Ideally we would know the type, but Repo mixes types.
                        // We can just print the blob size or raw hex.
                        println!("('{}', Blob[{} bytes])", key_hex, value_guard.value().len());
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
        Commands::Ref {
            ref_name,
            entity,
            attribute,
        } => {
            let target_ref_name = ref_name.as_deref().unwrap_or(ROOT_REF_NAME);
            if let (Some(entity), Some(attribute)) = (entity, attribute) {
                if let Some(value) = db.read_ref(target_ref_name, entity, attribute)? {
                    println!(
                        "Read at ref {}: ({}, {}, {})",
                        target_ref_name, entity, attribute, value
                    );
                } else {
                    println!(
                        "EAV triple ('{}', '{}') not found in ref",
                        entity, attribute
                    );
                }
            } else {
                println!("Displaying Tree for '{}':", target_ref_name);
                db.print_ref_recursive(target_ref_name)?;
            }
        }
        Commands::Stat => {
            db.stat()?;
        }
        Commands::Commit => {
            println!("Not yet implemented");
        }
        Commands::Gc => {
            println!("Not yet implemented");
        }
        Commands::Serve => {
            serve::run()?;
        }
        Commands::Sync { endpoint } => sync::run(*endpoint)?,
    }

    Ok(())
}
