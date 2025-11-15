use anyhow::Result;
use clap::{Parser, Subcommand};
use pika::init;
use pika::import;
use pika::serve;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version)]
struct Cli {
    db: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init { schema: PathBuf },
    Import { data: PathBuf, mapping: PathBuf },
    Serve,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Init { schema } => init::run(&args.db, schema),
        Commands::Import {
            data: data_path,
            mapping: mapping_path,
        } => import::run(&args.db, data_path, mapping_path),
        Commands::Serve => serve::run(&args.db),
    }
}
