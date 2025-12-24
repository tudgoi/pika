use clap::{Parser, Subcommand};
use redb::{Database, TableDefinition, ReadableDatabase, ReadableTable};
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
    List,
}

const TABLE: TableDefinition<&str, &str> = TableDefinition::new("my_data");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write { key, value } => {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(TABLE)?;
                table.insert(key.as_str(), value.as_str())?;
            }
            write_txn.commit()?;
            println!("Successfully wrote key-value pair ('{}', '{}') to database at: {:?}", key, value, args.db_path);
        }
        Commands::Read { key } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            if let Some(read_value) = table.get(key.as_str())? {
                println!("Read from DB: ('{}', '{}')", key, read_value.value());
            } else {
                println!("Key '{}' not found in database.", key);
            }
        }
        Commands::List => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(TABLE)?;
            println!("Listing all items in DB:");
            for result in table.iter()? {
                let (key, value) = result?;
                println!("('{}', '{}')", key.value(), value.value());
            }
        }
    }

    Ok(())
}
