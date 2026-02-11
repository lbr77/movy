use clap::{Parser, Subcommand};

use crate::{analysis::AnlaysisArgs, sui::SuiArgs};
use std::io::IsTerminal;

mod analysis;
mod aptos;
mod sui;

#[derive(Subcommand)]
pub enum MovySubcommand {
    Sui(SuiArgs),
    Analysis(AnlaysisArgs), // Aptos(AptosArgs)
}

#[derive(Parser)]
pub struct MovyCommand {
    #[clap(subcommand)]
    pub cmd: MovySubcommand,
}

async fn main_entry() {
    let args = MovyCommand::parse();
    match args.cmd {
        MovySubcommand::Sui(args) => args.run().await.expect("sui command failed"),
        MovySubcommand::Analysis(args) => args.run().await.expect("analysis failed"),
    }
}

fn main() {
    let use_colors = std::io::stdout().is_terminal() && std::io::stderr().is_terminal();
    color_eyre::install().unwrap();
    if let Ok(dot_file) = std::env::var("DOT") {
        dotenvy::from_path(dot_file).expect("can not read dotenvy");
    } else {
        // Allows failure
        let _ = dotenvy::dotenv();
    }
    let sub = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env()
                .expect("env contains non-utf8"),
        )
        .with_ansi(use_colors)
        .finish();
    tracing::subscriber::set_global_default(sub).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("can not build tokio")
        .block_on(main_entry())
}
