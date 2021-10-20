use crate::scoped_task::ScopedTaskSet;
use futures::prelude::*;
use http::Uri;
use rupnp::{
    ssdp::{SearchTarget, URN},
    Service,
};
use std::{fmt, io, net, time::Duration};

pub struct PortForwarder {
    tasks: ScopedTaskSet,
}

impl PortForwarder {
    pub fn new(port: u16) -> Self {
        let tasks = ScopedTaskSet::default();

        tasks.spawn(async move {
            log::info!(
                "UPnP starting port forwarding: EXT:{} -> INT:{}",
                port,
                port
            );
            let result = Self::run(port, port).await;
            // Warning, because we don't actually expect this to happen.
            log::warn!("UPnP port forwarding ended ({:?})", result)
        });

        Self { tasks }
    }

    async fn run(internal_port: u16, external_port: u16) -> Result<(), rupnp::Error> {
        // TODO: It would probably be better if we were specific here that we're looking for an IGD
        // device.
        let devices = rupnp::discover(&SearchTarget::RootDevice, Duration::from_secs(3)).await?;
        let mut devices = Box::pin(devices);

        let mut tasks = Vec::new();

        while let Some(device) = devices.try_next().await? {
            if let Some((service, version)) = find_connection_service(&device) {
                let per_igd_port_forwarder = PerIGDPortForwarder {
                    device_url: device.url().clone(),
                    service,
                    internal_port,
                    external_port,
                    version,
                };

                tasks.push(async move {
                    let r = per_igd_port_forwarder.run().await;
                    log::warn!("UPnP port forwarding on IGD ended ({:?})", r)
                });
            }
        }

        futures::future::join_all(tasks).await;

        Ok(())
    }
}

enum Version {
    V1,
    V2,
}

struct PerIGDPortForwarder {
    device_url: Uri,
    service: Service,
    internal_port: u16,
    external_port: u16,
    version: Version,
}

impl PerIGDPortForwarder {
    async fn run(self) -> Result<(), rupnp::Error> {
        let local_ip = local_address_to(&self.device_url).await?;

        let lease_duration = Duration::from_secs(180);
        let sleep_duration = Duration::from_secs(170);

        let mut ext_port_reported = false;
        let mut ext_addr_reported = false;

        loop {
            self.add_port_mapping(&local_ip, lease_duration).await?;

            if !ext_port_reported {
                ext_port_reported = true;
                println!(
                    "UPnP port forwarding started on external port {}",
                    self.external_port
                );
            }

            if !ext_addr_reported {
                ext_addr_reported = true;
                match self.get_external_ip_address().await {
                    Ok(addr) => {
                        println!("UPnP the external IP address is {}", addr);
                    }
                    Err(e) => {
                        log::warn!("UPnP failed to retrieve external IP address: {:?}", e);
                    }
                }
            }

            tokio::time::sleep(sleep_duration).await;
        }
    }

    // For IGDv1 see Section 2.4.16 in
    // https://openconnectivity.org/wp-content/uploads/2015/11/UPnP_IGD_WANIPConnection-1.0.pdf
    //
    // For IGDv2 see Section 2.5.16 in
    // https://upnp.org/specs/gw/UPnP-gw-WANIPConnection-v2-Service.pdf
    //
    // TODO: Consider also implementing AddAnyPortMapping when on IGDv2 for cases when the
    // requested external port is not free.
    async fn add_port_mapping(
        &self,
        local_ip: &net::IpAddr,
        lease_duration: Duration,
    ) -> Result<(), rupnp::Error> {
        let lease_duration = if lease_duration == Duration::ZERO {
            Duration::ZERO
        } else {
            std::cmp::max(Duration::from_secs(1), lease_duration)
        };

        const MAPPING_DESCRIPTION: &str = "OuiSync";

        let args = format!(
            "<NewRemoteHost></NewRemoteHost>\
            <NewEnabled>1</NewEnabled>\
            <NewExternalPort>{}</NewExternalPort>\
            <NewProtocol>TCP</NewProtocol>\
            <NewInternalPort>{}</NewInternalPort>\
            <NewInternalClient>{}</NewInternalClient>\
            <NewPortMappingDescription>{}</NewPortMappingDescription>\
            <NewLeaseDuration>{}</NewLeaseDuration>",
            self.external_port,
            self.internal_port,
            local_ip,
            MAPPING_DESCRIPTION,
            lease_duration.as_secs()
        );

        self.service
            .action(&self.device_url, "AddPortMapping", &args)
            .await?;

        Ok(())
    }

    // For IGDv1 see Section 2.4.18 in
    // https://openconnectivity.org/wp-content/uploads/2015/11/UPnP_IGD_WANIPConnection-1.0.pdf
    //
    // For IGDv2 see Section 2.5.20 in
    // https://upnp.org/specs/gw/UPnP-gw-WANIPConnection-v2-Service.pdf
    async fn get_external_ip_address(&self) -> Result<net::IpAddr, rupnp::Error> {
        let result = self
            .service
            .action(&self.device_url, "GetExternalIPAddress", "")
            .await?;

        match result.get("NewExternalIPAddress") {
            Some(addr) => match addr.parse::<net::IpAddr>() {
                Ok(addr) => Ok(addr),
                Err(_) => Err(InvalidResponse("failed to parse IP address").into()),
            },
            None => Err(InvalidResponse("response has no NewExternalIPAddress field").into()),
        }
    }
}

fn find_versioned_connection_service(
    device: &rupnp::DeviceSpec,
    version: u32,
) -> Option<&rupnp::Service> {
    const SCHEMA: &str = "schemas-upnp-org";

    device
        .find_device(&URN::device(SCHEMA, "WANDevice", version))
        .and_then(|device| device.find_device(&URN::device(SCHEMA, "WANConnectionDevice", version)))
        .and_then(|device| device.find_service(&URN::service(SCHEMA, "WANIPConnection", version)))
}

fn find_connection_service(device: &rupnp::DeviceSpec) -> Option<(rupnp::Service, Version)> {
    find_versioned_connection_service(&device, 2)
        .map(|service| (service.clone(), Version::V2))
        .or_else(|| {
            find_versioned_connection_service(&device, 1)
                .map(|service| (service.clone(), Version::V1))
        })
}

async fn local_address_to(url: &Uri) -> io::Result<net::IpAddr> {
    use std::io::{Error, ErrorKind};
    use std::net::SocketAddr;

    let remote_addr = {
        let host = match url.host() {
            Some(host) => host,
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("Failed to get the host part from URL {:?}", url),
                ))
            }
        };

        if let Ok(addr) = host.parse::<net::IpAddr>() {
            if let Some(port) = url.port_u16() {
                SocketAddr::new(addr, port)
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("Failed to parse PORT from URL {:?}", url),
                ));
            }
        } else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Failed to parse IP from URL {:?}", url),
            ));
        }
    };

    let any: SocketAddr = {
        if remote_addr.ip().is_ipv4() {
            (net::Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (net::Ipv6Addr::UNSPECIFIED, 0).into()
        }
    };

    let socket = tokio::net::UdpSocket::bind(any).await?;
    socket.connect(remote_addr).await?;
    socket.local_addr().map(|addr| addr.ip())
}

#[derive(Debug)]
struct InvalidResponse(&'static str);

impl From<InvalidResponse> for rupnp::Error {
    fn from(err: InvalidResponse) -> Self {
        rupnp::Error::invalid_response(err)
    }
}

impl fmt::Display for InvalidResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for InvalidResponse {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
