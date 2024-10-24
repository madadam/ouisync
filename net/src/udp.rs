pub use self::implementation::*;

use std::net::Ipv4Addr;

// Selected at random but to not clash with some reserved ones:
// https://www.iana.org/assignments/multicast-addresses/multicast-addresses.xhtml
pub const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 137);
pub const MULTICAST_PORT: u16 = 9271;

#[cfg(not(feature = "simulation"))]
mod implementation {
    use super::*;
    use crate::socket::{self, ReuseAddr};
    use std::{
        io,
        net::{SocketAddr, SocketAddrV4},
    };

    pub struct UdpSocket(tokio::net::UdpSocket);

    impl UdpSocket {
        /// Binds UDP socket to the given address. If the port is taken, uses a random one,
        pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
            Ok(Self(socket::bind(addr).await?))
        }

        pub async fn bind_multicast(interface: Ipv4Addr) -> io::Result<Self> {
            let socket: tokio::net::UdpSocket = socket::bind_with_reuse_addr(
                SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, MULTICAST_PORT).into(),
                ReuseAddr::Required,
            )
            .await?;
            socket.join_multicast_v4(MULTICAST_ADDR, interface)?;

            Ok(Self(socket))
        }

        pub async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            self.0.send_to(buf, target).await
        }

        pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            self.0.recv_from(buf).await
        }

        pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
            self.0.into_std()
        }
    }
}

#[cfg(feature = "simulation")]
mod implementation {
    use super::*;
    use std::{io, net::SocketAddr};

    pub struct UdpSocket;

    impl UdpSocket {
        pub async fn bind(_addr: SocketAddr) -> io::Result<Self> {
            unimplemented!("simulated udp sockets not supported")
        }

        pub async fn bind_multicast(_interface: Ipv4Addr) -> io::Result<Self> {
            unimplemented!("simulated udp sockets not supported")
        }

        pub async fn send_to(&self, _buf: &[u8], _target: SocketAddr) -> io::Result<usize> {
            unimplemented!()
        }

        pub async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            unimplemented!()
        }

        pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
            unimplemented!()
        }
    }
}
