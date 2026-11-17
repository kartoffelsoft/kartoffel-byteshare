use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub enum ServerEvent {
    Register
}

pub struct Server {
    port: u16,
    task: Option<JoinHandle<()>>,
    
    events_tx: mpsc::Sender<ServerEvent>,
    events_rx: Option<mpsc::Receiver<ServerEvent>>,
    stop_tx: Option<oneshot::Sender<()>>,

    ipv6_available: bool,
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
            ipv6_available: false,
        }
    }

    pub fn set_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub async fn run(mut self) -> anyhow::Result<Self> {
        let ipv4_socket_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.port);
        let ipv4_listener = tokio::net::TcpListener::bind(ipv4_socket_addr).await?;

        let port = ipv4_listener.local_addr()?.port();
        
        let ipv6_socket_addr = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port);
        let ipv6_listener =  {
            let result = (|| -> anyhow::Result<tokio::net::TcpListener> {
                let socket = socket2::Socket::new(
                    socket2::Domain::IPV6,
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )?;
                socket.set_only_v6(true)?;
                socket.set_reuse_address(true)?;
                socket.set_nonblocking(true)?;
                socket.bind(&ipv6_socket_addr.into())?;
                socket.listen(1024)?;
                
                Ok(tokio::net::TcpListener::from_std(socket.into())?)
            })();

            match result {
                Ok(listener) => Some(listener),
                Err(_) => {
                    None
                }
            }
        };

        self.ipv6_available = ipv6_listener.is_some();
        
        let cancel = CancellationToken::new();
        let connections = TaskTracker::new();

        self.task = Some(tokio::spawn(async move {
            let cancel = cancel.clone();
            let connections = connections.clone();
        }));

        Ok(self)
    }

    pub fn take_events(&mut self) -> mpsc::Receiver<ServerEvent> {
        self.events_rx
            .take()
            .expect("events_rx already taken")
    }
}
