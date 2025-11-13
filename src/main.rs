use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::{Result};
use pika::init;

#[derive(Parser)]
#[command(version)]
struct Cli {
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
        Commands::Init { schema } => init::run(schema),
    }
}
