use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::{KeyGen, Workload};
use fantoch::config::Config;
use fantoch::id::ProcessId;
use fantoch::planet::Planet;
use fantoch_exp::{
    ExperimentConfig, FantochFeature, PlacementFlat, Protocol, RunMode,
    SerializationFormat, Testbed,
};
use fantoch_plot::ResultsDB;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PreviousKeyGen {
    ConflictPool {
        conflict_rate: usize,
        pool_size: usize,
    },
    Zipf {
        coefficient: f64,
        total_keys_per_shard: usize,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PreviousWorkload {
    /// number of shards
    shard_count: u64,
    // key generator
    key_gen: PreviousKeyGen,
    /// number of keys accessed by the command
    keys_per_command: usize,
    /// number of commands to be submitted in this workload
    commands_per_client: usize,
    /// percentage of read-only commands
    read_only_percentage: usize,
    /// size of payload in command (in bytes)
    payload_size: usize,
    /// number of commands already issued in this workload
    command_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousConfig {
    /// number of processes
    n: usize,
    /// number of tolerated faults
    f: usize,
    /// number of shards
    shard_count: usize,
    /// if enabled, then execution is skipped
    execute_at_commit: bool,
    /// defines the interval between executor cleanups
    executor_cleanup_interval: Duration,
    /// defines the interval between between executed notifications sent to
    /// the local worker process
    executor_executed_notification_interval: Duration,
    /// defines whether the executor should monitor pending commands, and if
    /// so, the interval between each monitor
    executor_monitor_pending_interval: Option<Duration>,
    /// defines whether the executor should monitor the execution order of
    /// commands
    executor_monitor_execution_order: bool,
    /// defines the interval between garbage collections
    gc_interval: Option<Duration>,
    /// starting leader process
    leader: Option<ProcessId>,
    /// defines whether protocols (atlas, epaxos and tempo) should employ the
    /// NFR optimization
    nfr: bool,
    /// defines whether tempo should employ tiny quorums or not
    tempo_tiny_quorums: bool,
    /// defines the interval between clock bumps, if any
    tempo_clock_bump_interval: Option<Duration>,
    /// defines the interval the sending of `MDetached` messages in tempo, if
    /// any
    tempo_detached_send_interval: Option<Duration>,
    /// defines whether caesar should employ the wait condition
    caesar_wait_condition: bool,
    /// defines whether protocols should try to bypass the fast quorum process
    /// ack (which is only possible if the fast quorum size is 2)
    skip_fast_ack: bool,
}

#[derive(Deserialize, Serialize)]
pub struct PreviousExperimentConfig {
    pub placement: PlacementFlat,
    pub planet: Option<Planet>,
    pub run_mode: RunMode,
    pub features: Vec<FantochFeature>,
    pub testbed: Testbed,
    pub protocol: Protocol,
    pub config: PreviousConfig,
    pub clients_per_region: usize,
    pub workload: PreviousWorkload,
    pub batch_max_size: usize,
    pub batch_max_delay: Duration,
    pub process_tcp_nodelay: bool,
    pub tcp_buffer_size: usize,
    pub tcp_flush_interval: Option<Duration>,
    pub process_channel_buffer_size: usize,
    pub cpus: usize,
    pub workers: usize,
    pub executors: usize,
    pub multiplexing: usize,
    pub client_tcp_nodelay: bool,
    pub client_channel_buffer_size: usize,
}

fn main() -> Result<(), Report> {
    for results_dir in vec![
        // "/home/mari/eurosys_results/results_fairness_and_tail_latency",
        // "/home/mari/eurosys_results/results_increasing_load",
        // "/home/mari/eurosys_results/results_partial_replication",
        // "/home/mari/eurosys_results/results_batching",
        // "/home/mari/thesis_results/results_increasing_sites",
        // "/home/mari/thesis_results/results_fast_path",
    ] {
        // load results
        let timestamps =
            ResultsDB::list_timestamps(results_dir).wrap_err("load results")?;

        for timestamp in timestamps {
            // read the configuration of this experiment
            let exp_config_path =
                format!("{}/exp_config.json", timestamp.path().display());
            println!("migrating {}", exp_config_path);
            let previous: Result<PreviousExperimentConfig, _> =
                fantoch_exp::deserialize(
                    &exp_config_path,
                    SerializationFormat::Json,
                )
                .wrap_err_with(|| {
                    format!(
                        "deserialize experiment config of {:?}",
                        timestamp.path().display()
                    )
                });

            match previous {
                Ok(previous) => {
                    let key_gen = match previous.workload.key_gen {
                        PreviousKeyGen::ConflictPool {
                            conflict_rate,
                            pool_size,
                        } => KeyGen::ConflictPool {
                            conflict_rate,
                            pool_size,
                        },
                        PreviousKeyGen::Zipf {
                            coefficient,
                            total_keys_per_shard,
                        } => KeyGen::Zipf {
                            coefficient,
                            total_keys_per_shard,
                        },
                    };

                    // create workoad
                    let mut workload = Workload::new(
                        previous.workload.shard_count as usize,
                        key_gen,
                        previous.workload.keys_per_command,
                        previous.workload.commands_per_client,
                        previous.workload.payload_size,
                    );
                    // NOTE THAT `set_read_only_percentage` IS REQUIRED!
                    workload.set_read_only_percentage(
                        previous.workload.read_only_percentage,
                    );

                    let mut config =
                        Config::new(previous.config.n, previous.config.f);
                    config.set_shard_count(previous.config.shard_count);
                    config.set_execute_at_commit(
                        previous.config.execute_at_commit,
                    );
                    config.set_executor_cleanup_interval(
                        previous.config.executor_cleanup_interval,
                    );
                    config.set_executor_executed_notification_interval(
                        previous.config.executor_executed_notification_interval,
                    );
                    config.set_executor_monitor_pending_interval(
                        previous.config.executor_monitor_pending_interval,
                    );
                    config.set_executor_monitor_execution_order(
                        previous.config.executor_monitor_execution_order,
                    );
                    config.set_gc_interval(previous.config.gc_interval);
                    config.set_leader(previous.config.leader);
                    config.set_nfr(previous.config.nfr);
                    config.set_tempo_tiny_quorums(
                        previous.config.tempo_tiny_quorums,
                    );
                    config.set_tempo_clock_bump_interval(
                        previous.config.tempo_clock_bump_interval,
                    );
                    config.set_tempo_detached_send_interval(
                        previous.config.tempo_detached_send_interval,
                    );
                    config.set_caesar_wait_condition(
                        previous.config.caesar_wait_condition,
                    );
                    config.set_skip_fast_ack(previous.config.skip_fast_ack);

                    let exp_config = ExperimentConfig {
                        placement: previous.placement,
                        planet: previous.planet,
                        run_mode: previous.run_mode,
                        features: previous.features,
                        testbed: previous.testbed,
                        protocol: previous.protocol,
                        config,
                        clients_per_region: previous.clients_per_region,
                        process_tcp_nodelay: previous.process_tcp_nodelay,
                        tcp_buffer_size: previous.tcp_buffer_size,
                        tcp_flush_interval: previous.tcp_flush_interval,
                        process_channel_buffer_size: previous
                            .process_channel_buffer_size,
                        cpus: previous.cpus,
                        workers: previous.workers,
                        executors: previous.executors,
                        multiplexing: previous.multiplexing,
                        workload,
                        batch_max_size: previous.batch_max_size,
                        batch_max_delay: previous.batch_max_delay,
                        client_tcp_nodelay: previous.client_tcp_nodelay,
                        client_channel_buffer_size: previous
                            .client_channel_buffer_size,
                    };

                    // save experiment config
                    fantoch_exp::serialize(
                        exp_config,
                        &exp_config_path,
                        SerializationFormat::Json,
                    )
                    .wrap_err("migrate_exp_config")?;
                }
                Err(e) => {
                    let missing_file =
                        String::from("No such file or directory (os error 2)");
                    if e.root_cause().to_string() == missing_file {
                        // if some file was not found, it may be because the
                        // folder is empty; in this case, ignore the
                        // error
                        println!("entry ignored...");
                    } else {
                        // if not, quit
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok(())
}
