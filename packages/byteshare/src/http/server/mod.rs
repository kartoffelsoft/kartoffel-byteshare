use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub enum ServerEvent {
    Register
}

pub struct Server {
    port: u16,
    task: Option<JoinHandle<()>>,
    
    events_tx: mpsc::Sender<ServerEvent>,
    events_rx: Option<mpsc::Receiver<ServerEvent>>,

    stop_tx: Option<oneshot::Sender<()>>,
}

impl Server {
    pub fn new() -> Self {
        let (events_tx, events_rx) = mpsc::channel(16);

        Self {
            port: 54321,
            task: None,
            events_tx: events_tx,
            events_rx: Some(events_rx),
            stop_tx: None,
        }
    }

    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub async fn run(mut self) -> anyhow::Result<Self> {
        let ipv4_socket_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.port);
  
        self.task = Some(tokio::spawn(async move {
            
        }));

        Ok(self)
    }

    pub fn take_events(&mut self) -> mpsc::Receiver<ServerEvent> {
        self.events_rx
            .take()
            .expect("events_rx already taken")
    }
}
