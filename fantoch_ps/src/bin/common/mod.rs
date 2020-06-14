/// This modules contains common functionality to parse protocol arguments.
#[allow(dead_code)]
pub mod protocol;

use fantoch::config::Config;

const DEFAULT_TRANSITIVE_CONFLICTS: bool = false;
const DEFAULT_EXECUTE_AT_COMMIT: bool = false;
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_TCP_BUFFER_SIZE: usize = 8 * 1024; // 8 KBs
const DEFAULT_GC_INTERVAL: usize = 500; // milliseconds
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 10000;

#[allow(dead_code)]
pub fn tokio_runtime() -> tokio::runtime::Runtime {
    // get number of cpus
    let cpus = num_cpus::get();
    let reserved = fantoch::run::INDEXES_RESERVED;
    let threads = cpus - reserved;
    println!(
        "cpus: {} | reserved: {} | runtime threads: {}",
        cpus, reserved, threads
    );

    // create tokio runtime
    tokio::runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(threads)
        .enable_io()
        .enable_time()
        .thread_name("runner")
        .build()
        .expect("tokio runtime build should work")
}

pub fn parse_config(
    n: Option<&str>,
    f: Option<&str>,
    transitive_conflicts: Option<&str>,
    execute_at_commit: Option<&str>,
    gc_interval: Option<&str>,
) -> Config {
    let n = n
        .expect("n should be set")
        .parse::<usize>()
        .expect("n should be a number");
    let f = f
        .expect("f should be set")
        .parse::<usize>()
        .expect("f should be a number");
    let transitive_conflicts = transitive_conflicts
        .map(|transitive_conflicts| {
            transitive_conflicts
                .parse::<bool>()
                .expect("transitive conflicts should be a bool")
        })
        .unwrap_or(DEFAULT_TRANSITIVE_CONFLICTS);
    let execute_at_commit = execute_at_commit
        .map(|execute_at_commit| {
            execute_at_commit
                .parse::<bool>()
                .expect("execute_at_commit should be a bool")
        })
        .unwrap_or(DEFAULT_EXECUTE_AT_COMMIT);
    let gc_interval = gc_interval
        .map(|gc_interval| {
            gc_interval
                .parse::<usize>()
                .expect("gc_interval should be a number")
        })
        .unwrap_or(DEFAULT_GC_INTERVAL);
    // create config
    let mut config = Config::new(n, f);
    // set transitive conflicts and skip execution
    config.set_transitive_conflicts(transitive_conflicts);
    config.set_execute_at_commit(execute_at_commit);
    config.set_garbage_collection_interval(gc_interval);
    config
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

pub fn parse_tcp_flush_interval(flush_interval: Option<&str>) -> Option<usize> {
    flush_interval.map(|flush_interval| {
        flush_interval
            .parse::<usize>()
            .expect("flush interval should be a number")
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

pub fn parse_execution_log(execution_log: Option<&str>) -> Option<String> {
    execution_log.map(String::from)
}
