mod db;
mod mst;
mod pt;

use crate::{
    db::{print_mst_recursive},
    mst::{hex_string},
    pt::{print_pt_recursive},
};
use clap::{Parser, Subcommand, ValueEnum};
use redb::{ReadableDatabase, ReadableTable, TableHandle};
use std::path::PathBuf;

use db::{Db, EAV_TABLE, ROOT_REF_NAME, REFS_TABLE, REPO_TABLE, Engine};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the redb database file
    db_path: PathBuf,

    /// Storage engine to use
    #[arg(short, long, value_enum, default_value_t = Engine::Mst)]
    engine: Engine,

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
        /// The name of the reference to display. Defaults to root ref for selected engine.
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
    let db = Db::new(&args.db_path, args.engine)?;

    match &args.command {
        Commands::Write {
            entity,
            attribute,
            value,
        } => {
            // Call repo.write() instead of inline logic
            db.write(entity, attribute, value)?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?} using {:?}",
                entity, attribute, value, args.db_path, args.engine
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
        Commands::Ref { ref_name } => {
            let read_txn = db.redb.begin_read()?;
            let refs_table = read_txn.open_table(REFS_TABLE)?;

            let target_ref_name = ref_name.as_deref().unwrap_or(ROOT_REF_NAME);

            println!("Displaying Tree for '{}':", target_ref_name);
            if let Some(root_hash) =
                refs_table.get(target_ref_name)?.map(|guard| *guard.value())
            {
                match args.engine {
                    Engine::Mst => print_mst_recursive(&db.redb, root_hash)?,
                    Engine::Pt => print_pt_recursive(&db.redb, root_hash)?,
                }
            } else {
                println!("No root found for '{}'.", target_ref_name);
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
