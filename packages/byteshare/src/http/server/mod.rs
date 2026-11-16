use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub enum ServerEvent {
    Register {

    }
}

pub struct Server {
    port: u16,
    task: Option<JoinHandle<()>>,
    
    events_tx: mpsc::Sender<ServerEvent>,
    events_rx: Option<mpsc::Receiver<ServerEvent>>,

    stop_tx: Option<oneshot::Sender<()>>,
}

impl Server {
    pub fn new(port: u16) -> Self {
        let (events_tx, events_rx) = mpsc::channel(16);

        Self {
            port,
            task: None,
            events_tx: events_tx,
            events_rx: Some(events_rx),
            stop_tx: None,
        }
    }

    pub fn take_events(&mut self) -> mpsc::Receiver<ServerEvent> {
        self.events_rx
            .take()
            .expect("events_rx already taken")
    }
}
