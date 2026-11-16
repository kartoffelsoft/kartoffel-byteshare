mod keyboard;

use crate::storage;
use crate::{Args, Command};
use byteshare::http::server::{Server, ServerEvent}; 
use keyboard::{Keyboard, KeyboardEvent, KeyCode, KeyModifiers};
use std::ops::ControlFlow;
use std::sync::Arc;

pub enum AppEvent {
    Keyboard(KeyboardEvent),
    Server(ServerEvent),
}

struct App {
    server: Arc<Server>,
}

impl App {
    async fn dispatch(&mut self, event: AppEvent) -> ControlFlow<()> {
        match event {
            AppEvent::Keyboard(event) => {
                if event.code == KeyCode::Char('c') && event.modifiers.control {
                    return ControlFlow::Break(())      
                }
                self.handle_keyboard(event).await;
            }
            AppEvent::Server(event) => {
                self.handle_server(event).await;
            }
            _ => {}
        }

        ControlFlow::Continue(())
    }

    async fn handle_keyboard(&mut self, event: KeyboardEvent) {
    }

    async fn handle_server(&mut self, event: ServerEvent) {
        match event {
            ServerEvent::Register => {}
        }
    }
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    run_default().await
}

async fn run_default() -> anyhow::Result<()> {
    let mut server = Server::new()
        .run()
        .await?;
    let mut server_events = server.take_events();
    
    let mut keyboard = Keyboard::new()?;
    let mut keyboard_events = keyboard.take_events();

    let mut app = App {
        server: Arc::new(server),
    };

    loop {
        let event = tokio::select! {
            Some(event) = server_events.recv() => AppEvent::Server(event),
            Some(event) = keyboard_events.recv() => AppEvent::Keyboard(event),
            else => break,
        };

        if app.dispatch(event).await.is_break() {
            break;
        }
    }

    Ok(())
}
