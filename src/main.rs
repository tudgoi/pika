use anyhow::Result;
use clap::{Parser, Subcommand};
use pika::init;
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
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Init { schema } => init::run(args.db, schema),
    }
}
