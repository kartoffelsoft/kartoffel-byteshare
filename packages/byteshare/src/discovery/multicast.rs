use crate::utils::net::{find_net_interfaces};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
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

}

struct MulticastState {
    sockets: RwLock<Vec<MulticastSocket>>,
}

struct MulticastSocket {
    target: SocketAddr,
    socket: Arc<UdpSocket>,
}

pub(crate) struct Multicast {
    config: Config,
    state: Arc<MulticastState>,
    task: Mutex<Option<JoinHandle<()>>>,
    events_rx: Option<mpsc::Receiver<MulticastEvent>>,
}

impl Multicast {
    pub(super) fn new(config: Config) -> Self{
        Self {
            config,
            state: Arc::new(MulticastState{
                sockets: RwLock::new(Vec::new()),
            }),
            task: Mutex::new(None),
            events_rx: None,
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

        *self.state.sockets.write().await = sockets;

        let cancel = CancellationToken::new();
 
        let task = tokio::spawn({
            let cancel = cancel.clone();
            async move {

            }
        });

        *self.task.lock().await = Some(task);
        Ok(())
    }

    pub(super) fn take_events(&mut self) -> mpsc::Receiver<MulticastEvent> {
        self.events_rx.take().expect("events_rx already taken")
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
                    target: SocketAddr::from(SocketAddrV4::new(group, port)),
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
                    target: SocketAddr::from(SocketAddrV6::new(group_v6, port, 0, intf.index)),
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