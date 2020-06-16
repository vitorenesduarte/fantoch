use super::Rw;
use crate::log;
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    ip_addr: Option<IpAddr>,
    rw: Rw<TcpStream>,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        tcp_nodelay: bool,
        tcp_buffer_size: usize,
    ) -> Self {
        // get ip addr
        let ip_addr = stream.peer_addr().ok().map(|peer_addr| peer_addr.ip());
        // configure stream
        configure(&stream, tcp_nodelay, tcp_buffer_size);
        // create rw
        let rw = Rw::from(tcp_buffer_size, tcp_buffer_size, stream);
        Self { ip_addr, rw }
    }

    pub fn ip_addr(&self) -> Option<IpAddr> {
        self.ip_addr
    }
}

fn configure(stream: &TcpStream, tcp_nodelay: bool, tcp_buffer_size: usize) {
    // set TCP_NODELAY
    stream
        .set_nodelay(tcp_nodelay)
        .expect("setting TCP_NODELAY should work");

    // // maybe adapt SO_RCVBUF and SO_SNDBUF and compute buffer capacity
    // // change SO_RCVBUF if lower than `tcp_buffer_size`
    // if let Ok(so_rcvbuf) = stream.recv_buffer_size() {
    //     if so_rcvbuf < tcp_buffer_size {
    //         stream
    //             .set_recv_buffer_size(tcp_buffer_size)
    //             .expect("setting tcp recv buffer should work");
    //     }
    // }
    // log!("SO_RCVBUF: {:?}", stream.recv_buffer_size());

    // // change SO_SNFBUF if lower than `tcp_buffer_size`
    // if let Ok(so_sndbuf) = stream.send_buffer_size() {
    //     if so_sndbuf < tcp_buffer_size {
    //         stream
    //             .set_send_buffer_size(tcp_buffer_size)
    //             .expect("setting tcp send buffer should work");
    //     }
    // }
    // log!("SO_SNDBUF: {:?}", stream.send_buffer_size());
}

impl Deref for Connection {
    type Target = Rw<TcpStream>;

    fn deref(&self) -> &Self::Target {
        &self.rw
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rw
    }
}
