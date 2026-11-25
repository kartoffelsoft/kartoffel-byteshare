use crate::utils::net::{find_net_interfaces};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct Config {
    pub group: Ipv4Addr,
    pub group_v6: Option<Ipv6Addr>, 
    pub(crate) port: u16,
}

pub(crate) enum MulticastEvent {
    Packet {
        ip: IpAddr,
        scope_id: Option<u32>,
        size: usize,
        payload: Vec<u8>,
    },
    Err,
}

struct MulticastState {
    sockets: RwLock<Vec<MulticastSocket>>,
}

#[derive(Clone)]
struct MulticastSocket {
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
}

#[derive(Clone)]
pub(crate) struct MulticastHandle {
    state: Arc<MulticastState>,
    cancel: CancellationToken,
}

impl MulticastHandle {
    pub(crate) async fn send(&self, payload: &Vec<u8>) {
        let sockets = self.state.sockets.read().await.clone();
        for MulticastSocket { addr, socket} in sockets {
            if let Err(err) = socket.send_to(&payload, addr).await {
                log::trace!("failed to send multicast to {addr}: {err:#}");
            }
        }
    }

    pub(crate) fn stop(self) {
        self.cancel.cancel();
    }
}

pub(crate) struct Multicast {
    config: Config,
    state: Arc<MulticastState>,

    cancel: CancellationToken,
    events_rx: Option<mpsc::Receiver<MulticastEvent>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Multicast {
    pub(super) fn new(config: Config) -> Self{
        Self {
            config,
            state: Arc::new(MulticastState{
                sockets: RwLock::new(Vec::new()),
            }),
            cancel: CancellationToken::new(),
            events_rx: None,
            task: Mutex::new(None),
        }
    }

    pub(super) async fn run(&mut self) -> anyhow::Result<()> {
        let (events_tx, events_rx) = mpsc::channel(64);
        self.events_rx = Some(events_rx);

        let sockets = bind_sockets(
            self.config.group,
            self.config.group_v6,
            self.config.port,
        )?;
        
        if sockets.is_empty() {
            anyhow::bail!(
                "no network interface available for multicast on port {}", 
                self.config.port,
            );
        }

        *self.state.sockets.write().await = sockets.clone();

        let task = tokio::spawn({
            let cancel = self.cancel.clone();
            async move {
                let mut tasks = tokio::task::JoinSet::new();
                for socket in sockets {
                    tasks.spawn(run_socket_receive(
                        socket,
                        events_tx.clone(),
                        cancel.clone(),
                    ));
                }

                tokio::select! {
                    _ = async { while tasks.join_next().await.is_some() {} } => {
                    }
                }
                
                tasks.abort_all();
            }
        });

        *self.task.lock().await = Some(task);
        Ok(())
    }

    pub(crate) fn handle(&mut self) -> MulticastHandle {
        MulticastHandle {
            state: self.state.clone(),
            cancel: self.cancel.clone(),
        }
    }

    pub(crate) fn take_events(&mut self) -> mpsc::Receiver<MulticastEvent> {
        self.events_rx.take().expect("events_rx already taken")
    }
}

async fn run_socket_receive(
    socket: MulticastSocket,
    events_tx: mpsc::Sender<MulticastEvent>,
    cancel: CancellationToken,
) {
    let mut buffer = vec![0u8; 65536];
    let mut error_count = 0;

    loop {
        let result = tokio::select! {
            result = socket.socket.recv_from(&mut buffer) => result,
            _ = cancel.cancelled() => return,
        };

        let (size, addr) = match result {
            Ok(result) => {
                error_count = 0;
                result
            }
            Err(err) => {
                error_count += 1;
                log::trace!("cound not receive message on interface: {err:#}");
                if error_count >= 10 {
                    log::trace!("too many errors. give up");
                    return;
                }
                continue;
            }
        };

        let event = MulticastEvent::Packet {
            ip: addr.ip(),
            scope_id: match &addr {
                SocketAddr::V6(addr) => {
                    let scope_ip = addr.scope_id();
                    if scope_ip == 0 {
                        return;
                    }
                    Some(addr.scope_id())
                },
                _ => None,
            },
            size,
            payload: buffer.clone(),
        };

        if events_tx.send(event).await.is_err() {
            log::trace!("failed to send mutlicast event");
            return;
        }
    }
}

fn bind_sockets(
    group: Ipv4Addr,
    group_v6: Option<Ipv6Addr>, 
    port: u16,
) -> anyhow::Result<Vec<MulticastSocket>> {
    let intfs = find_net_interfaces()?;

    let mut result = Vec::new();
    for intf in intfs.v4 {
        match bind_socket_v4(group, port, intf.address) {
            Ok(socket) => {
                log::trace!(
                    "succeed binding multicast socket (interface: {} ({}), group: {group}, port: {port}", 
                    intf.name, 
                    intf.address,
                );
                result.push(MulticastSocket {
                    addr: SocketAddr::from(SocketAddrV4::new(group, port)),
                    socket: Arc::new(socket),
                });
            }
            Err(err) => {
                log::trace!(
                    "failed to binding multicast socket (interface: {} ({}), group: {group}, port: {port}): {err:#}",
                    intf.name, 
                    intf.address,
                )
            }
        }
    }

    let Some(group_v6) = group_v6 else {
        return Ok(result);
    };

    for intf in intfs.v6 {
        match bind_socket_v6(group_v6, port, intf.index) {
            Ok(socket) => {
                log::trace!(
                    "succeed binding multicast socket (interface: {} ({}), group: {group}, port: {port}", 
                    intf.name, 
                    intf.index,
                );
                result.push(MulticastSocket {
                    addr: SocketAddr::from(SocketAddrV6::new(group_v6, port, 0, intf.index)),
                    socket: Arc::new(socket),
                });
            }
            Err(err) => {
                log::trace!(
                    "failed to binding multicast socket (interface: {} ({}), group: {group}, port: {port}): {err:#}",
                    intf.name, 
                    intf.index,
                )
            }
        }
    }
    
    Ok(result)
}

fn bind_socket_v4(
    group: Ipv4Addr, 
    port: u16, 
    interface: Ipv4Addr
) -> anyhow::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).into())?;
    socket.join_multicast_v4(&group, &interface)?;
    socket.set_multicast_if_v4(&interface)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_multicast_ttl_v4(1)?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}

fn bind_socket_v6(
    group: Ipv6Addr,
    port: u16, 
    interface: u32,
) -> anyhow::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddr::from(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, port, 0, 0)).into())?;
    socket.join_multicast_v6(&group, interface)?;
    socket.set_multicast_if_v6(interface)?;
    socket.set_multicast_loop_v6(true)?;
    socket.set_multicast_hops_v6(1)?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}
