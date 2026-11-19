use std::net::{Ipv4Addr, Ipv6Addr};
use tokio::sync::{mpsc};

pub(crate) struct Config {
    pub group: Ipv4Addr,
    pub group_v6: Option<Ipv6Addr>, 
    pub(crate) port: u16,
}

pub(crate) enum MulticastEvent {

}

pub(crate) struct Multicast {
    config: Config,
    events_rx: Option<mpsc::Receiver<MulticastEvent>>,
}

impl Multicast {
    pub(super) fn new(config: Config) -> Self{
        Self {
            config,
            events_rx: None,
        }
    }

    pub(super) async fn run(&mut self) {
        let (events_tx, events_rx) = mpsc::channel(64);
        self.events_rx = Some(events_rx);
    }

    pub(super) fn take_events(&mut self) -> mpsc::Receiver<MulticastEvent> {
        self.events_rx.take().expect("events_rx already taken")
    }
}