use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const TCP_KEEPALIVE: socket2::TcpKeepalive = socket2::TcpKeepalive::new()
    .with_time(std::time::Duration::from_secs(60))
    .with_interval(std::time::Duration::from_secs(10));

pub enum ServerEvent {
    Register
}

pub struct ServerHandle {
    cancel: CancellationToken,
}

impl ServerHandle {
    pub fn stop(self) {
        self.cancel.cancel();
    }
}

pub struct Server {
    task: Mutex<Option<JoinHandle<()>>>,
    
    cancel: CancellationToken,
    events_tx: mpsc::Sender<ServerEvent>,
    events_rx: Option<mpsc::Receiver<ServerEvent>>,
    ipv6_available: bool,
    port: u16,
}

impl Server {
    pub fn new() -> Self {
        let (events_tx, events_rx) = mpsc::channel(16);

        Self {
            task: Mutex::new(None),
            cancel: CancellationToken::new(),
            events_tx: events_tx,
            events_rx: Some(events_rx),
            ipv6_available: false,
            port: 51117,
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

        let tracker = TaskTracker::new();

        let task = tokio::spawn({
            let cancel = self.cancel.clone();
            let tracker = tracker.clone();
            async move {
                tokio::select! {
                    result = run_server(ipv4_listener, cancel.clone(), tracker) => {

                    }
                }
            }
        });

        *self.task.lock().await = Some(task);

        Ok(())
    }

    pub fn handle(self) -> ServerHandle {
        ServerHandle {
            cancel: self.cancel.clone()
        }
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
        let conn = tokio::select! {
            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        let _ = stream.set_nodelay(true);
                        let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&TCP_KEEPALIVE);
                        (stream, addr)
                    }
                    Err(err) => {
                        continue;
                    }
                }
            }
            _ = cancel.cancelled() => {
                break;
            }
        };

        let cancel = cancel.clone();

        tracker.spawn(async move {
            tokio::select! {
                _ = serve_connection(conn.0, conn.1) => {}
                _ = cancel.cancelled() => {}
            }
        });
    }

    Ok(())
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
            (&Method::POST, "/api/register") => {
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
