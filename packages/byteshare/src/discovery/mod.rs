mod multicast;

use multicast::{Multicast};
use std::net::{Ipv4Addr, Ipv6Addr};

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

pub struct Discovery {
    config: Config,
}

impl Discovery {
    pub fn new(config: Config) -> Self {
        Self {
            config,
        }
    }

    pub async fn run(&mut self) {
        let mut multicast = Multicast::new(multicast::Config {
            group: self.config.group,
            group_v6: self.config.group_v6,
            port: self.config.port,
        });
        multicast.run().await;
        let mut multicast_events = multicast.take_events();

        tokio::spawn({
            async move {
                loop {
                    let event = match multicast_events.recv().await {
                        Some(event) => event,
                        None => break,
                    };

                    match event {

                    }
                }
            }
        });
    }
}
