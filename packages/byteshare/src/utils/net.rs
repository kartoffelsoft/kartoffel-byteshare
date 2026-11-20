use std::net::{IpAddr, Ipv4Addr};

pub(crate) struct NetIntfV4 {
    pub(crate) name: String,
    pub(crate) address: Ipv4Addr,
}

pub(crate) struct NetIntfV6 {
    pub(crate) name: String,
    pub(crate) index: u32,
}

pub(crate) struct NetIntfs {
    pub(crate) v4:Vec<NetIntfV4>,
    pub(crate) v6:Vec<NetIntfV6>,
}

 pub(crate) fn find_net_interfaces() -> std::io::Result<NetIntfs> {
    struct Intf {
        name: String,
        index: Option<u32>,
        addresses: Vec<IpAddr>,
    }

    let mut intfs: Vec<Intf> = Vec::new();

    for addr in if_addrs::get_if_addrs()? {
        if addr.is_loopback() {
            continue;
        }

        match intfs.iter_mut().find(|intf| intf.name == addr.name) {
            Some(intf) => {
                intf.index = intf.index.or(addr.index);
                intf.addresses.push(addr.ip());
            }
            None => intfs.push(Intf {
                name: addr.name.clone(),
                index: addr.index,
                addresses: vec![addr.ip()],
            })
        }
    }

    let mut result = NetIntfs {
        v4: Vec::new(),
        v6: Vec::new(),
    };

    for intf in intfs {
        let mut is_v6 = false;
        for address in &intf.addresses {
            match address {
                IpAddr::V4(address) => result.v4.push(NetIntfV4 {
                    name: intf.name.clone(),
                    address: *address,
                }),
                IpAddr::V6(_) => is_v6 = true,
            }
        }    

        if is_v6 {
            match intf.index {
                Some(index) => result.v6.push(NetIntfV6 {
                    name: intf.name,
                    index,
                }),
                None => {
                    log::trace!("Interface {} has no index", intf.name)
                },
            }
        }
    }

    Ok(result)
 }
