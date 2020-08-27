/// This modules contains common functionality to parse protocol arguments.
#[allow(dead_code)]
pub mod protocol;

use std::fs::File;
use std::path::Path;
use std::time::Duration;

const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_TCP_BUFFER_SIZE: usize = 8 * 1024; // 8 KBs
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 10000;
const DEFAULT_STACK_SIZE: usize = 2 * 1024 * 1024; // 2MBs

#[allow(dead_code)]
pub fn tokio_runtime(
    stack_size: usize,
    cpus: Option<usize>,
) -> tokio::runtime::Runtime {
    // get number of cpus
    let available = num_cpus::get();
    let cpus = cpus.unwrap_or(available);
    tracing::info!("cpus: {} of {}", cpus, available);

    // create tokio runtime
    tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(cpus)
        .thread_stack_size(stack_size)
        .enable_io()
        .enable_time()
        .thread_name("runner")
        .build()
        .expect("tokio runtime build should work")
}

struct TestWriter {
    file: File,
}

impl TestWriter {
    fn new(log_file: impl AsRef<Path>) -> Self {
        let file =
            File::create(log_file).expect("creating log file should work");
        Self { file }
    }
}

impl std::io::Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        self.file.write_all(buf)?;
        Ok(buf_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[must_use]
pub fn init_tracing_subscriber(
    log_file: Option<impl AsRef<Path> + std::fmt::Debug>,
) -> tracing_appender::non_blocking::WorkerGuard {
    println!("log_file: {:?}", log_file);
    let format = tracing_subscriber::fmt::format()
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false);

    let builder = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(0)
        .lossy(false);
    let (non_blocking_appender, guard) = match log_file {
        Some(log_file) => {
            builder.finish(tracing_appender::rolling::never(".", log_file))
        }
        None => builder.finish(std::io::stdout()),
    };

    tracing_subscriber::fmt()
        .event_format(format)
        .with_writer(non_blocking_appender)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    guard
}

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

pub fn parse_tcp_flush_interval(interval: Option<&str>) -> Option<Duration> {
    interval.map(|interval| {
        let millis = interval
            .parse::<u64>()
            .expect("flush interval should be a number");
        Duration::from_millis(millis)
    })
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

pub fn parse_stack_size(stack_size: Option<&str>) -> usize {
    stack_size
        .map(|stack_size| {
            stack_size
                .parse::<usize>()
                .expect("stack size should be a number")
        })
        .unwrap_or(DEFAULT_STACK_SIZE)
}

pub fn parse_cpus(cpus: Option<&str>) -> Option<usize> {
    cpus.map(|cpus| cpus.parse::<usize>().expect("cpus should be a number"))
}
