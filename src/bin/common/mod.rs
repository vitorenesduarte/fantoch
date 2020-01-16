/// This modules contains common functionality to parse protocol arguments.
#[allow(dead_code)]
pub mod protocol;

const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 10000;

pub fn parse_tcp_nodelay(tcp_nodelay: Option<&str>) -> bool {
    tcp_nodelay
        .map(|tcp_nodelay| {
            tcp_nodelay
                .parse::<bool>()
                .expect("tcp_nodelay should be a boolean")
        })
        .unwrap_or(DEFAULT_TCP_NODELAY)
}

pub fn parse_channel_buffer_size(channel_buffer_size: Option<&str>) -> usize {
    channel_buffer_size
        .map(|channel_buffer_size| {
            channel_buffer_size
                .parse::<usize>()
                .expect("channel_buffer_size should be a number")
        })
        .unwrap_or(DEFAULT_CHANNEL_BUFFER_SIZE)
}
