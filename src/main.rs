use anyhow::Result;
use clap::{Parser, Subcommand};
use pika::chu;
use pika::import;
use pika::init;
use pika::serve;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        db: PathBuf,
        schema: PathBuf,
    },
    Import {
        db: PathBuf,
        data: PathBuf,
        mapping: PathBuf,
    },
    Serve {
        db: PathBuf,
    },
    Chu,
}

fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    match args.command {
        Commands::Init {
            db: db_path,
            schema: schema_path,
        } => init::run(&db_path, schema_path),
        Commands::Import {
            db: db_path,
            data: data_path,
            mapping: mapping_path,
        } => import::run(&db_path, data_path, mapping_path),
        Commands::Serve { db: db_path } => serve::run(db_path),
        Commands::Chu => chu::run(),
    }
}
