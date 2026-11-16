mod keyboard;

use crate::storage;
use crate::{Args, Command};
use byteshare::http::server::{Server, ServerEvent}; 
use keyboard::{Keyboard, KeyboardEvent};
use std::sync::Arc;

pub enum AppEvent {
    Keyboard(KeyboardEvent),
    Server(ServerEvent),
}

struct App {
    server: Arc<Server>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    run_default().await
}

async fn run_default() -> anyhow::Result<()> {
    let mut server = Server::new(1234);
    let mut server_events = server.take_events();
    
    let mut keyboard = Keyboard::new()?;
    let mut keyboard_events = keyboard.take_events();

    let mut app = App {
        server: Arc::new(server),
    };

    let mut quit = false;

    while !quit {
        let event = tokio::select! {
            Some(event) = server_events.recv() => AppEvent::Server(event),
            Some(event) = keyboard_events.recv() => AppEvent::Keyboard(event),
        };
    }

    Ok(())
}
