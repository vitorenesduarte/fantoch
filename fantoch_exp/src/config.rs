#[cfg(feature = "exp")]
use crate::args;
use crate::{FantochFeature, Protocol, RunMode, Testbed};
use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::{Planet, Region};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// FIXED
#[cfg(feature = "exp")]
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
const PROCESS_TCP_FLUSH_INTERVAL: Option<usize> = None;

// if this value is 100, the run doesn't finish, which probably means there's a
// deadlock somewhere with 1000 we can see that channels fill up sometimes with
// 10000 that doesn't seem to happen
// - in AWS 10000 is not enough; setting it to 100k
// - in Apollo with 32k clients per site, 100k is not enough at the fpaxos
//   leader; setting it to 1M
// - in Apollo with 16k clients per site, 1M is not enough with newt; setting it
//   to 100M (since 10M is also not enough)
const CHANNEL_BUFFER_SIZE: usize = 100_000_000;

#[cfg(feature = "exp")]
const EXECUTION_LOG: Option<String> = None;
#[cfg(feature = "exp")]
const TRACER_SHOW_INTERVAL: Option<usize> = None;
#[cfg(feature = "exp")]
const PING_INTERVAL: Option<usize> = Some(500); // every 500ms

// if paxos, set process 1 as the leader
const LEADER: ProcessId = 1;

// client tcp config
const CLIENT_TCP_NODELAY: bool = true;

#[cfg(feature = "exp")]
pub struct ProtocolConfig {
    id: ProcessId,
    sorted: Option<Vec<ProcessId>>,
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
    metrics_file: String,
}

#[cfg(feature = "exp")]
impl ProtocolConfig {
    pub fn new(
        protocol: Protocol,
        id: ProcessId,
        mut config: Config,
        sorted: Option<Vec<ProcessId>>,
        ips: Vec<(String, Option<usize>)>,
        metrics_file: &str,
    ) -> Self {
        let (workers, executors) =
            workers_executors_and_leader(protocol, &mut config);

        Self {
            id,
            sorted,
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
            metrics_file: metrics_file.to_string(),
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
        if let Some(sorted) = self.sorted.as_ref() {
            // make sorted ids comma-separted
            let sorted = sorted
                .iter()
                .map(|process_id| process_id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            args.extend(args!["--sorted", sorted]);
        }
        if let Some(interval) = self.config.gc_interval() {
            args.extend(args!["--gc_interval", interval.as_millis()]);
        }
        if let Some(leader) = self.config.leader() {
            args.extend(args!["--leader", leader]);
        }
        args.extend(args![
            "--newt_tiny_quorums",
            self.config.newt_tiny_quorums()
        ]);
        if let Some(interval) = self.config.newt_clock_bump_interval() {
            args.extend(args![
                "--newt_clock_bump_interval",
                interval.as_millis()
            ]);
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
        args.extend(args!["--metrics_file", self.metrics_file]);
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

fn workers_executors_and_leader(
    protocol: Protocol,
    config: &mut Config,
) -> (usize, usize) {
    // for all protocol but newt, create a single executor
    match protocol {
        Protocol::AtlasLocked => (WORKERS + EXECUTORS, 1),
        Protocol::EPaxosLocked => (WORKERS + EXECUTORS, 1),
        Protocol::FPaxos => {
            // in the case of paxos, also set a leader
            config.set_leader(LEADER);
            (WORKERS + EXECUTORS, 1)
        }
        Protocol::NewtAtomic => (WORKERS, EXECUTORS),
        Protocol::NewtLocked => (WORKERS, EXECUTORS),
        Protocol::NewtFineLocked => (WORKERS, EXECUTORS),
        Protocol::Basic => (WORKERS, EXECUTORS),
    }
}

#[cfg(feature = "exp")]
pub struct ClientConfig {
    id_start: usize,
    id_end: usize,
    ips: Vec<String>,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    metrics_file: String,
}

#[cfg(feature = "exp")]
impl ClientConfig {
    pub fn new(
        id_start: usize,
        id_end: usize,
        ips: Vec<String>,
        workload: Workload,
        metrics_file: &str,
    ) -> Self {
        Self {
            id_start,
            id_end,
            ips,
            workload,
            tcp_nodelay: CLIENT_TCP_NODELAY,
            channel_buffer_size: CHANNEL_BUFFER_SIZE,
            metrics_file: metrics_file.to_string(),
        }
    }

    pub fn to_args(&self) -> Vec<String> {
        use fantoch::client::{KeyGen, ShardGen};
        let shard_gen = match self.workload.shard_gen() {
            ShardGen::Random { shard_count } => {
                format!("random,{}", shard_count)
            }
        };
        let key_gen = match self.workload.key_gen() {
            KeyGen::ConflictRate { conflict_rate } => {
                format!("conflict_rate,{}", conflict_rate)
            }
            KeyGen::Zipf {
                coefficient,
                key_count,
            } => format!("zipf,{},{}", coefficient, key_count),
        };
        args![
            "--ids",
            format!("{}-{}", self.id_start, self.id_end),
            "--addresses",
            self.ips_to_addresses(),
            "--shards_per_command",
            self.workload.shards_per_command(),
            "--shard_gen",
            shard_gen,
            "--keys_per_shard",
            self.workload.keys_per_shard(),
            "--key_gen",
            key_gen,
            "--commands_per_client",
            self.workload.commands_per_client(),
            "--payload_size",
            self.workload.payload_size(),
            "--tcp_nodelay",
            self.tcp_nodelay,
            "--channel_buffer_size",
            self.channel_buffer_size,
            "--metrics_file",
            self.metrics_file,
        ]
    }

    fn ips_to_addresses(&self) -> String {
        self.ips
            .iter()
            .map(|ip| format!("{}:{}", ip, CLIENT_PORT))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Deserialize, Serialize)]
pub struct ExperimentConfig {
    pub regions: HashMap<Region, ProcessId>,
    pub planet: Option<Planet>,
    pub run_mode: RunMode,
    pub features: Vec<FantochFeature>,
    pub testbed: Testbed,
    pub protocol: Protocol,
    pub config: Config,
    pub clients_per_region: usize,
    pub workload: Workload,
    pub process_tcp_nodelay: bool,
    pub tcp_buffer_size: usize,
    pub tcp_flush_interval: Option<usize>,
    pub process_channel_buffer_size: usize,
    pub workers: usize,
    pub executors: usize,
    pub multiplexing: usize,
    pub client_tcp_nodelay: bool,
    pub client_channel_buffer_size: usize,
}

impl ExperimentConfig {
    pub fn new(
        regions: HashMap<Region, ProcessId>,
        planet: Option<Planet>,
        run_mode: RunMode,
        features: Vec<FantochFeature>,
        testbed: Testbed,
        protocol: Protocol,
        mut config: Config,
        clients_per_region: usize,
        workload: Workload,
    ) -> Self {
        let (workers, executors) =
            workers_executors_and_leader(protocol, &mut config);

        Self {
            regions,
            planet,
            run_mode,
            features,
            testbed,
            protocol,
            config,
            clients_per_region,
            process_tcp_nodelay: PROCESS_TCP_NODELAY,
            tcp_buffer_size: PROCESS_TCP_BUFFER_SIZE,
            tcp_flush_interval: PROCESS_TCP_FLUSH_INTERVAL,
            process_channel_buffer_size: CHANNEL_BUFFER_SIZE,
            workers,
            executors,
            multiplexing: MULTIPLEXING,
            workload,
            client_tcp_nodelay: CLIENT_TCP_NODELAY,
            client_channel_buffer_size: CHANNEL_BUFFER_SIZE,
        }
    }
}

impl fmt::Debug for ExperimentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "config = {:?}", self.config)?;
        writeln!(f, "protocol = {:?}", self.protocol)?;
        writeln!(f, "clients_per_region = {:?}", self.clients_per_region)?;
        writeln!(f, "workload = {:?}", self.workload)
    }
}
