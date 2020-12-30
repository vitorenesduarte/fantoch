#[cfg(feature = "exp")]
use crate::args;
use crate::{FantochFeature, Protocol, RunMode, Testbed};
use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::id::{ProcessId, ShardId};
use fantoch::planet::{Planet, Region};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub type RegionIndex = usize;
pub type Placement = HashMap<(Region, ShardId), (ProcessId, RegionIndex)>;
pub type PlacementFlat = Vec<(Region, ShardId, ProcessId, RegionIndex)>;

// FIXED
#[cfg(feature = "exp")]
const IP: &str = "0.0.0.0";

// parallelism config
const WORKERS: usize = 16;
const EXECUTORS: usize = 16;
const MULTIPLEXING: usize = 32;

// process tcp config
const PROCESS_TCP_NODELAY: bool = true;
// by default, each socket stream is buffered (with a buffer of size 8KBs),
// which should greatly reduce the number of syscalls for small-sized messages
const PROCESS_TCP_BUFFER_SIZE: usize = 5 * 1024 * 1024; // 5MB
const PROCESS_TCP_FLUSH_INTERVAL: Option<usize> = Some(2);

// if this value is 100, the run doesn't finish, which probably means there's a
// deadlock somewhere with 1000 we can see that channels fill up sometimes with
// 10000 that doesn't seem to happen
// - in AWS 10000 is not enough; setting it to 100k
// - in Apollo with 32k clients per site, 100k is not enough at the fpaxos
//   leader; setting it to 1M
// - in Apollo with 16k clients per site, 1M is not enough with newt; setting it
//   to 100M (since 10M is also not enough)
const PROCESS_CHANNEL_BUFFER_SIZE: usize = 100_000_000;
const CLIENT_CHANNEL_BUFFER_SIZE: usize = 10_000;

// tokio config
#[cfg(feature = "exp")]
const PROCESS_STACK_SIZE: Option<usize> = Some(32 * 1024 * 1024); // 32MB
#[cfg(feature = "exp")]
const CLIENT_STACK_SIZE: Option<usize> = None; // default is 8MB

#[cfg(feature = "exp")]
const EXECUTION_LOG: Option<String> = None;
#[cfg(feature = "exp")]
const TRACER_SHOW_INTERVAL: Option<usize> = None;
#[cfg(feature = "exp")]
const PING_INTERVAL: Option<usize> = Some(500); // every 500ms

#[cfg(feature = "exp")]
// const STATUS_FREQUENCY: Option<usize> = None;
const STATUS_FREQUENCY: Option<usize> = Some(10);

// if paxos, set process 1 as the leader
const LEADER: ProcessId = 1;

// client tcp config
const CLIENT_TCP_NODELAY: bool = true;

#[cfg(feature = "exp")]
pub struct ProtocolConfig {
    process_id: ProcessId,
    shard_id: ShardId,
    sorted: Option<Vec<(ProcessId, ShardId)>>,
    ips: Vec<(ProcessId, String, Option<usize>)>,
    config: Config,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<usize>,
    process_channel_buffer_size: usize,
    client_channel_buffer_size: usize,
    workers: usize,
    executors: usize,
    multiplexing: usize,
    execution_log: Option<String>,
    tracer_show_interval: Option<usize>,
    ping_interval: Option<usize>,
    metrics_file: String,
    stack_size: Option<usize>,
    cpus: usize,
    log_file: String,
}

#[cfg(feature = "exp")]
impl ProtocolConfig {
    pub fn new(
        protocol: Protocol,
        process_id: ProcessId,
        shard_id: ShardId,
        mut config: Config,
        sorted: Option<Vec<(ProcessId, ShardId)>>,
        ips: Vec<(ProcessId, String, Option<usize>)>,
        metrics_file: String,
        cpus: usize,
        log_file: String,
    ) -> Self {
        let (workers, executors) =
            workers_executors_and_leader(protocol, &mut config);

        Self {
            process_id,
            shard_id,
            sorted,
            ips,
            config,
            tcp_nodelay: PROCESS_TCP_NODELAY,
            tcp_buffer_size: PROCESS_TCP_BUFFER_SIZE,
            tcp_flush_interval: PROCESS_TCP_FLUSH_INTERVAL,
            process_channel_buffer_size: PROCESS_CHANNEL_BUFFER_SIZE,
            client_channel_buffer_size: CLIENT_CHANNEL_BUFFER_SIZE,
            workers,
            executors,
            multiplexing: MULTIPLEXING,
            execution_log: EXECUTION_LOG,
            tracer_show_interval: TRACER_SHOW_INTERVAL,
            ping_interval: PING_INTERVAL,
            metrics_file,
            stack_size: PROCESS_STACK_SIZE,
            cpus,
            log_file,
        }
    }

    pub fn set_tracer_show_interval(&mut self, interval: usize) {
        self.tracer_show_interval = Some(interval);
    }

    pub fn to_args(&self) -> Vec<String> {
        let mut args = args![
            "--id",
            self.process_id,
            "--shard_id",
            self.shard_id,
            "--ip",
            IP,
            "--port",
            port(self.process_id),
            "--client_port",
            client_port(self.process_id),
            "--addresses",
            self.ips_to_addresses(),
            "--processes",
            self.config.n(),
            "--faults",
            self.config.f(),
            "--shard_count",
            self.config.shard_count(),
            "--execute_at_commit",
            self.config.execute_at_commit(),
        ];
        if let Some(sorted) = self.sorted.as_ref() {
            // make sorted ids comma-separted
            let sorted = sorted
                .iter()
                .map(|(process_id, shard_id)| {
                    format!("{}-{}", process_id, shard_id)
                })
                .collect::<Vec<_>>()
                .join(",");
            args.extend(args!["--sorted", sorted]);
        }
        args.extend(args![
            "--executor_cleanup_interval",
            self.config.executor_cleanup_interval().as_millis()
        ]);
        if let Some(interval) = self.config.executor_monitor_pending_interval()
        {
            args.extend(args![
                "--executor_monitor_pending_interval",
                interval.as_millis()
            ]);
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
        if let Some(interval) = self.config.newt_detached_send_interval() {
            args.extend(args![
                "--newt_detached_send_interval",
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
            "--process_channel_buffer_size",
            self.process_channel_buffer_size,
            "--client_channel_buffer_size",
            self.client_channel_buffer_size,
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
        if let Some(stack_size) = self.stack_size {
            args.extend(args!["--stack_size", stack_size]);
        }
        args.extend(args!["--cpus", self.cpus, "--log_file", self.log_file]);
        args
    }

    fn ips_to_addresses(&self) -> String {
        self.ips
            .iter()
            .map(|(peer_id, ip, delay)| {
                let address = format!("{}:{}", ip, port(*peer_id));
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
    let we = |executors| (WORKERS + EXECUTORS - executors, executors);
    // for all protocol but newt, create a single executor
    match protocol {
        // 1 extra executor for partial replication
        Protocol::AtlasLocked => we(2),
        // 1 extra executor for partial replication (although, not implemented
        // yet)
        Protocol::EPaxosLocked => we(2),
        // Caesar implementation is sequential for both workers and executors
        Protocol::Caesar => (1, 1),
        Protocol::FPaxos => {
            // in the case of paxos, also set a leader
            config.set_leader(LEADER);
            we(1)
        }
        Protocol::NewtAtomic => we(EXECUTORS),
        Protocol::NewtLocked => we(EXECUTORS),
        Protocol::Basic => we(EXECUTORS),
    }
}

#[cfg(feature = "exp")]
pub struct ClientConfig {
    id_start: usize,
    id_end: usize,
    ips: Vec<(ProcessId, String)>,
    workload: Workload,
    tcp_nodelay: bool,
    channel_buffer_size: usize,
    status_frequency: Option<usize>,
    metrics_file: String,
    stack_size: Option<usize>,
    cpus: Option<usize>,
    log_file: String,
}

#[cfg(feature = "exp")]
impl ClientConfig {
    pub fn new(
        id_start: usize,
        id_end: usize,
        ips: Vec<(ProcessId, String)>,
        workload: Workload,
        metrics_file: String,
        log_file: String,
    ) -> Self {
        Self {
            id_start,
            id_end,
            ips,
            workload,
            tcp_nodelay: CLIENT_TCP_NODELAY,
            channel_buffer_size: CLIENT_CHANNEL_BUFFER_SIZE,
            status_frequency: STATUS_FREQUENCY,
            metrics_file,
            stack_size: CLIENT_STACK_SIZE,
            cpus: None,
            log_file,
        }
    }

    pub fn to_args(&self) -> Vec<String> {
        use fantoch::client::KeyGen;
        let key_gen = match self.workload.key_gen() {
            KeyGen::ConflictPool {
                conflict_rate,
                pool_size,
            } => {
                format!("conflict_pool,{},{}", conflict_rate, pool_size)
            }
            KeyGen::Zipf {
                coefficient,
                total_keys_per_shard,
            } => format!("zipf,{},{}", coefficient, total_keys_per_shard),
        };
        let mut args = args![
            "--ids",
            format!("{}-{}", self.id_start, self.id_end),
            "--addresses",
            self.ips_to_addresses(),
            "--shard_count",
            self.workload.shard_count(),
            "--key_gen",
            key_gen,
            "--keys_per_command",
            self.workload.keys_per_command(),
            "--commands_per_client",
            self.workload.commands_per_client(),
            "--payload_size",
            self.workload.payload_size(),
            "--read_only_percentage",
            self.workload.read_only_percentage(),
            "--tcp_nodelay",
            self.tcp_nodelay,
            "--channel_buffer_size",
            self.channel_buffer_size,
            "--metrics_file",
            self.metrics_file,
        ];
        if let Some(status_frequency) = self.status_frequency {
            args.extend(args!["--status_frequency", status_frequency]);
        }
        if let Some(stack_size) = self.stack_size {
            args.extend(args!["--stack_size", stack_size]);
        }
        if let Some(cpus) = self.cpus {
            args.extend(args!["--cpus", cpus]);
        }
        args.extend(args!["--log_file", self.log_file]);
        args
    }

    fn ips_to_addresses(&self) -> String {
        self.ips
            .iter()
            .map(|(process_id, ip)| {
                format!("{}:{}", ip, client_port(*process_id))
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Deserialize, Serialize)]
pub struct ExperimentConfig {
    pub placement: PlacementFlat,
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
    pub cpus: usize,
    pub workers: usize,
    pub executors: usize,
    pub multiplexing: usize,
    pub client_tcp_nodelay: bool,
    pub client_channel_buffer_size: usize,
}

impl ExperimentConfig {
    pub fn new(
        placement: Placement,
        planet: Option<Planet>,
        run_mode: RunMode,
        features: Vec<FantochFeature>,
        testbed: Testbed,
        protocol: Protocol,
        mut config: Config,
        clients_per_region: usize,
        workload: Workload,
        cpus: usize,
    ) -> Self {
        let (workers, executors) =
            workers_executors_and_leader(protocol, &mut config);

        // can't serialize to json with a key that is not a string, so let's
        // flat it
        let placement = placement
            .into_iter()
            .map(|((a, b), (c, d))| (a, b, c, d))
            .collect();
        Self {
            placement,
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
            process_channel_buffer_size: PROCESS_CHANNEL_BUFFER_SIZE,
            cpus,
            workers,
            executors,
            multiplexing: MULTIPLEXING,
            workload,
            client_tcp_nodelay: CLIENT_TCP_NODELAY,
            client_channel_buffer_size: CLIENT_CHANNEL_BUFFER_SIZE,
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

#[derive(Clone, Copy, Debug)]
pub enum ProcessType {
    Server(ProcessId),
    Client(usize),
}

impl ProcessType {
    pub fn name(&self) -> String {
        match self {
            Self::Server(process_id) => format!("server_{}", process_id),
            Self::Client(region_index) => format!("client_{}", region_index),
        }
    }
}

// create filename for a run file (which can be a log, metrics, dstats, etc,
// depending on the extension passed in)
pub fn run_file(process_type: ProcessType, file_ext: &str) -> String {
    format!("{}.{}", process_type.name(), file_ext)
}

// create filename prefix
pub fn file_prefix(process_type: ProcessType, region: &Region) -> String {
    format!("{:?}_{}", region, process_type.name())
}

const PORT: usize = 3000;
const CLIENT_PORT: usize = 4000;

pub fn port(process_id: ProcessId) -> usize {
    process_id as usize + PORT
}

pub fn client_port(process_id: ProcessId) -> usize {
    process_id as usize + CLIENT_PORT
}
