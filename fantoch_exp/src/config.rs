use crate::args;
use crate::exp::Protocol;
use fantoch::config::Config;
use fantoch::id::ProcessId;

// FIXED
const IP: &str = "0.0.0.0";
pub const PORT: usize = 3000;
pub const CLIENT_PORT: usize = 4000;

// parallelism config
const WORKERS: usize = 16;
const EXECUTORS: usize = 16;
const MULTIPLEXING: usize = 32;

// process tcp config
const PROCESS_TCP_NODELAY: bool = true;
// by default, each socket stream is buffered (with a buffer of size 8KBs),
// which should greatly reduce the number of syscalls for small-sized messages
const PROCESS_TCP_BUFFER_SIZE: usize = 8 * 1024;
const PROCESS_TCP_FLUSH_INTERVAL: Option<usize> = Some(2); // millis

// if this value is 100, the run doesn't finish, which probably means there's a
// deadlock somewhere with 1000 we can see that channels fill up sometimes with
// 10000 that doesn't seem to happen
// - in AWS 10000 is not enough; setting it to 100k
// - in Apollo with 32k clients per site, 100k is not enough at the fpaxos
//   leader; setting it to 1M
// - in Apollo with 16k clients per site, 1M is not enough with newt; setting it
//   to 100M (since 10M is also not enough)
const CHANNEL_BUFFER_SIZE: usize = 100_000_000;

const EXECUTION_LOG: Option<String> = None;
const TRACER_SHOW_INTERVAL: Option<usize> = None;
const PING_INTERVAL: Option<usize> = Some(500); // every 500ms

// if paxos, set process 1 as the leader
const LEADER: ProcessId = 1;

// clients config
const CONFLICT_RATE: usize = 10;
const COMMANDS_PER_CLIENT: usize = 500;
const PAYLOAD_SIZE: usize = 4 * 1024;

// client tcp config
const CLIENT_TCP_NODELAY: bool = true;

pub struct ProtocolConfig {
    id: ProcessId,
    ips: Vec<(String, Option<usize>)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
    ping_interval: Option<usize>,
}

impl ProtocolConfig {
    pub fn new(
        protocol: Protocol,
        id: ProcessId,
        mut config: Config,
        ips: Vec<(String, Option<usize>)>,
    ) -> Self {
        // for all protocol but newt, create a single executor
        let (workers, executors) = match protocol {
            Protocol::AtlasLocked => (WORKERS + EXECUTORS, 1),
            Protocol::EPaxosLocked => (WORKERS + EXECUTORS, 1),
            Protocol::FPaxos => {
                // in the case of paxos, also set a leader
                config.set_leader(LEADER);
                (WORKERS + EXECUTORS, 1)
            }
            Protocol::NewtAtomic => (WORKERS, EXECUTORS),
        };

        Self {
            id,
            ips,
            config,
            tcp_nodelay: PROCESS_TCP_NODELAY,
            tcp_buffer_size: PROCESS_TCP_BUFFER_SIZE,
            tcp_flush_interval: PROCESS_TCP_FLUSH_INTERVAL,
            channel_buffer_size: CHANNEL_BUFFER_SIZE,
            workers,
            executors,
            multiplexing: MULTIPLEXING,
            execution_log: EXECUTION_LOG,
            tracer_show_interval: TRACER_SHOW_INTERVAL,
            ping_interval: PING_INTERVAL,
        }
    }

    pub fn set_tracer_show_interval(&mut self, interval: usize) {
        self.tracer_show_interval = Some(interval);
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = args![
            "--id",
            self.id,
            "--ip",
            IP,
            "--port",
            PORT,
            "--client_port",
            CLIENT_PORT,
            "--addresses",
            self.ips_to_addresses(),
            "--processes",
            self.config.n(),
            "--faults",
            self.config.f(),
            "--transitive_conflicts",
            self.config.transitive_conflicts(),
            "--execute_at_commit",
            self.config.execute_at_commit(),
        ];
        if let Some(interval) = self.config.gc_interval() {
            args.extend(args!["--gc_interval", interval]);
        }
        if let Some(leader) = self.config.leader() {
            args.extend(args!["--leader", leader]);
        }
        args.extend(args![
            "--newt_tiny_quorums",
            self.config.newt_tiny_quorums()
        ]);
        if let Some(interval) = self.config.newt_clock_bump_interval() {
            args.extend(args!["--newt_clock_bump_interval", interval]);
        }
        args.extend(args!["--skip_fast_ack", self.config.skip_fast_ack()]);

        args.extend(args![
            "--tcp_nodelay",
            self.tcp_nodelay,
            "--tcp_buffer_size",
            self.tcp_buffer_size
        ]);
        if let Some(interval) = self.tcp_flush_interval {
            args.extend(args!["--tcp_flush_interval", interval]);
        }
        args.extend(args![
            "--channel_buffer_size",
            self.channel_buffer_size,
            "--workers",
            self.workers,
            "--executors",
            self.executors,
            "--multiplexing",
            self.multiplexing
        ]);
        if let Some(log) = &self.execution_log {
            args.extend(args!["--execution_log", log]);
        }
        if let Some(interval) = self.tracer_show_interval {
            args.extend(args!["--tracer_show_interval", interval]);
        }
        if let Some(interval) = self.ping_interval {
            args.extend(args!["--ping_interval", interval]);
        }
        args
    }

    fn ips_to_addresses(&self) -> String {
        self.ips
            .iter()
            .map(|(ip, delay)| {
                let address = format!("{}:{}", ip, PORT);
                if let Some(delay) = delay {
                    format!("{}-{}", address, delay)
                } else {
                    address
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

pub struct ClientConfig {
    id_start: usize,
    id_end: usize,
    ip: String,
    conflict_rate: usize,
    commands_per_client: usize,
    payload_size: usize,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
}

impl ClientConfig {
    pub fn new(id_start: usize, id_end: usize, ip: String) -> Self {
        Self {
            id_start,
            id_end,
            ip,
            conflict_rate: CONFLICT_RATE,
            commands_per_client: COMMANDS_PER_CLIENT,
            payload_size: PAYLOAD_SIZE,
            tcp_nodelay: CLIENT_TCP_NODELAY,
            channel_buffer_size: CHANNEL_BUFFER_SIZE,
        }
    }

    pub fn to_args(&self) -> Vec<String> {
        args![
            "--ids",
            format!("{}-{}", self.id_start, self.id_end),
            "--address",
            self.ip_to_address(),
            "--conflict_rate",
            self.conflict_rate,
            "--commands_per_client",
            self.commands_per_client,
            "--payload_size",
            self.payload_size,
            "--tcp_nodelay",
            self.tcp_nodelay,
            "--channel_buffer_size",
            self.channel_buffer_size,
        ]
    }

    fn ip_to_address(&self) -> String {
        format!("{}:{}", self.ip, CLIENT_PORT)
    }
}
