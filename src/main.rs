mod init;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

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

fn main() {
    let args = Cli::parse();

    match args.command {
        Commands::Init { schema } => init::run(schema),
    }
}
