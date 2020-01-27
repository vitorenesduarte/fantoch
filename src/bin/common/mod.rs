/// This modules contains common functionality to parse protocol arguments.
#[allow(dead_code)]
pub mod protocol;

const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_TCP_BUFFER_SIZE: usize = 8 * 1024; // 8 KBs
const DEFAULT_FLUSH_INTERVAL: usize = 100; // microseconds
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

pub fn parse_tcp_buffer_size(buffer_size: Option<&str>) -> usize {
    parse_buffer_size(buffer_size, DEFAULT_TCP_BUFFER_SIZE)
}

pub fn parse_tcp_flush_interval(flush_interval: Option<&str>) -> usize {
    flush_interval
        .map(|flush_interval| {
            flush_interval
                .parse::<usize>()
                .expect("flush interval should be a number")
        })
        .unwrap_or(DEFAULT_FLUSH_INTERVAL)
}

pub fn parse_channel_buffer_size(buffer_size: Option<&str>) -> usize {
    parse_buffer_size(buffer_size, DEFAULT_CHANNEL_BUFFER_SIZE)
}

fn parse_buffer_size(buffer_size: Option<&str>, default: usize) -> usize {
    buffer_size
        .map(|buffer_size| {
            buffer_size
                .parse::<usize>()
                .expect("buffer size should be a number")
        })
        .unwrap_or(default)
}
