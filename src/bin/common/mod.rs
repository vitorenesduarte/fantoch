/// This modules contains common functionality to parse protocol arguments.
#[allow(dead_code)]
pub mod protocol;

const DEFAULT_TCP_NODELAY: bool = true;

pub fn parse_tcp_nodelay(tcp_nodelay: Option<&str>) -> bool {
    tcp_nodelay
        .map(|tcp_nodelay| {
            tcp_nodelay
                .parse::<bool>()
                .expect("tcp_nodelay should be a boolean")
        })
        .unwrap_or(DEFAULT_TCP_NODELAY)
}
