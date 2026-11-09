use crate::{Args, Command};
use byteshare::http::server::ServerEvent;
use tokio::sync::{mpsc, oneshot};

pub enum AppEvent {
    Server(ServerEvent),
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    run_default().await
}

async fn run_default() -> anyhow::Result<()> {
    let (app_tx, app_rx) = mpsc::channel::<AppEvent>(64);

    Ok(())
}
