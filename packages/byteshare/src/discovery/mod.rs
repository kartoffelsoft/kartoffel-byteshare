mod multicast;

use multicast::{Multicast, MulticastEvent, MulticastHandle};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr};
use tokio_util::sync::CancellationToken;

const DEFAULT_MULTICAST_GROUP: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 11);
const DEFAULT_MULTICAST_GROUP_V6: Ipv6Addr = Ipv6Addr::new(0xff12, 0, 0, 0, 0, 0, 0xfd3a, 0xe420);
const DEFAULT_PORT: u16 = 51117;

pub struct Config {
    pub group: Ipv4Addr,
    pub group_v6: Option<Ipv6Addr>, 
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            group: DEFAULT_MULTICAST_GROUP,
            group_v6: Some(DEFAULT_MULTICAST_GROUP_V6),
            port: DEFAULT_PORT,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryMessage {
    pub alias: String,
    pub version: String,
    pub port: u16,
}

#[derive(Clone)]
pub struct DiscoveryHandle {
    cancel: CancellationToken
}

impl DiscoveryHandle {
    pub fn stop(self) {
        self.cancel.cancel();
    }
}

pub struct Discovery {
    config: Config,

    cancel: CancellationToken,
    multicast: Option<MulticastHandle>,
}

impl Discovery {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            cancel: CancellationToken::new(),
            multicast: None,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut multicast = Multicast::new(multicast::Config {
            group: self.config.group,
            group_v6: self.config.group_v6,
            port: self.config.port,
        });
        multicast.run().await?;
        let mut multicast_events = multicast.take_events();
        self.multicast = Some(multicast.handle());

        tokio::spawn({
            let cancel = self.cancel.clone();
            let multicast = self.multicast.clone().unwrap();
            async move {
                loop {
                    tokio::select! {
                        event = multicast_events.recv() => {
                            let Some(event) = event else {
                                break;
                            };

                            match event {
                                MulticastEvent::Packet {
                                    ip, 
                                    scope_id, 
                                    size,
                                    payload,
                                } => {
                                    let message = match serde_json::from_slice::<DiscoveryMessage>(&payload[..size]) {
                                        Ok(message) => message,
                                        Err(err) => {
                                            log::trace!("failed to parse multicast packet: {err:#}");
                                            continue;
                                        }
                                    };
                                }
                                MulticastEvent::Err => {

                                }
                            }
                        }
                        _ = cancel.cancelled() => {
                            multicast.stop();
                            break;
                        }
                    }
                }
            }
        });
        Ok(())
    }

    pub fn handle(&mut self) -> DiscoveryHandle {
        DiscoveryHandle { 
            cancel: self.cancel.clone() 
        }
    }

}
