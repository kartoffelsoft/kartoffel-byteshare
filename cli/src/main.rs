mod app;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Command {
    Send {
        #[arg(long, value_name = "TARGET")]
        to: Option<String>,

        #[arg(value_name = "PATH", required_unless_present = "text", num_args = 1..)]
        paths: Vec<PathBuf>,
    },
}

#[derive(Parser)]
#[command(name = "byte-share-cli", version, about, after_help = HELP_SECTIONS)]
pub struct Args {
    #[arg(long, env = "BYTE_SHARE_ALIAS")]
    pub alias: Option<String>,

    #[arg(long, env = "BYTE_SHARE_PORT")]
    pub port: Option<u16>,
    
    #[arg(long, env = "BYTE_SHARE_DESTINATION")]
    pub destination: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

const HELP_SECTIONS: &str = "";
                             
fn main() -> anyhow::Result<()> {
    println!("Hello, world!");
    let args = Args::parse();
    tokio::runtime::Runtime::new()?.block_on(app::run(args))
}
