use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const TCP_KEEPALIVE: socket2::TcpKeepalive = socket2::TcpKeepalive::new()
    .with_time(std::time::Duration::from_secs(60))
    .with_interval(std::time::Duration::from_secs(10));

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
            port: 51117,
            task: None,
            events_tx: events_tx,
            events_rx: Some(events_rx),
            stop_tx: None,
            ipv6_available: false,
        }
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
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
        let tracker = TaskTracker::new();

        self.task = Some(tokio::spawn({
            let cancel = cancel.clone();
            let tracker = tracker.clone();
            async move {
                tokio::select! {
                    result = run_server(ipv4_listener, cancel, tracker) => {

                    }
                    // _ = async {
                    // } => {}
                }
            }
        }));

        Ok(())
    }

    pub fn take_events(&mut self) -> mpsc::Receiver<ServerEvent> {
        self.events_rx
            .take()
            .expect("events_rx already taken")
    }
}

async fn run_server(
    listener: tokio::net::TcpListener,
    cancel: CancellationToken,
    tracker: TaskTracker
) -> anyhow::Result<()> {
    loop {
        let (tcp_stream, remote_addr) = match listener.accept().await {
            Ok(accepted) => {
                let _ = accepted.0.set_nodelay(true);
                let _ = socket2::SockRef::from(&accepted.0).set_tcp_keepalive(&TCP_KEEPALIVE);
                accepted
            }
            Err(err) => {
                continue;
            }
        };

        let cancel = cancel.clone();

        tracker.spawn(async move {
            tokio::select! {
                _ = serve_connection(tcp_stream, remote_addr) => {}
                _ = cancel.cancelled() => {}
            }
        });
    }
}

async fn serve_connection(
    tcp_stream: tokio::net::TcpStream,
    remote_addr: SocketAddr,
) {
    if let Err(err) = serve_http(tcp_stream).await {
        println!("failed to serve connection");
    }
}

async fn serve_http(tcp_stream: tokio::net::TcpStream) -> hyper::Result<()> {
    hyper::server::conn::http1::Builder::new()
        .serve_connection(
            TokioIo::new(tcp_stream),
            hyper::service::service_fn(move |mut req: Request<Incoming>| {
                serve_request(req)
            }),
        )
        .await
}

async fn serve_request(req: Request<Incoming>) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let result: Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> = async {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/ping") => {
                let body = Full::new(Bytes::from("pong"))
                    .map_err(|never| match never {})
                    .boxed();

                Ok(Response::new(body))
            }
            _ => {
                let mut response: Response<BoxBody<Bytes, hyper::Error>> = Response::new(
                    Full::new(Bytes::new())
                        .map_err(|never| match never {})
                    .boxed()
                );
                *response.status_mut() = StatusCode::NOT_FOUND;
                Ok(response)
            }
        }
    }.await;

    Ok(result.unwrap_or_else(|err| {
        let body = Full::new(Bytes::from("Internal Server Error"))
            .map_err(|never| match never {})
            .boxed();

        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(body)
            .unwrap()
    }))
}