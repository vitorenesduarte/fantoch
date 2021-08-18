// This module contains common data-structures between protocols.
pub mod common;

// This module contains the definition of `Atlas`.
mod atlas;

// This module contains the definition of `EPaxos`.
mod epaxos;

// This module contains the definition of `Tempo`.
mod tempo;

// This module contains the definition of `FPaxos`.
mod fpaxos;

// This module contains the definition of `Caesar`.
mod caesar;

// This module contains common functionality for partial replication.
mod partial;

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use caesar::CaesarLocked;
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use fpaxos::FPaxos;
pub use tempo::{TempoAtomic, TempoLocked, TempoSequential};

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{KeyGen, Workload};
    use fantoch::config::Config;
    use fantoch::executor::ExecutionOrderMonitor;
    use fantoch::id::{ProcessId, Rifl};
    use fantoch::kvs::Key;
    use fantoch::planet::Planet;
    use fantoch::protocol::{Protocol, ProtocolMetrics, ProtocolMetricsKind};
    use fantoch::run::tests::{run_test_with_inspect_fun, tokio_test_runtime};
    use fantoch::sim::Runner;
    use fantoch::HashMap;
    use std::time::Duration;

    // global test config
    const SHARD_COUNT: usize = 1;
    const KEY_GEN: KeyGen = KeyGen::ConflictPool {
        conflict_rate: 50,
        pool_size: 1,
    };
    const KEYS_PER_COMMAND: usize = 2;
    const READ_ONLY_PERCENTAGE: usize = 0;
    const COMMANDS_PER_CLIENT: usize = 100;
    const CLIENTS_PER_PROCESS: usize = 10;

    macro_rules! config {
        ($n:expr, $f:expr) => {{
            let config = Config::new($n, $f);
            config
        }};
        ($n:expr, $f:expr, $leader:expr) => {{
            let mut config = Config::new($n, $f);
            config.set_leader($leader);
            config
        }};
    }

    macro_rules! tempo_config {
        ($n:expr, $f:expr) => {{
            let mut config = Config::new($n, $f);
            // always set `tempo_detached_send_interval`
            config.set_tempo_detached_send_interval(Duration::from_millis(100));
            config
        }};
        ($n:expr, $f:expr, $clock_bump_interval:expr) => {{
            let mut config = tempo_config!($n, $f);
            config.set_tempo_tiny_quorums(true);
            config.set_tempo_clock_bump_interval($clock_bump_interval);
            config
        }};
    }

    macro_rules! caesar_config {
        ($n:expr, $f:expr, $wait:expr) => {{
            let mut config = Config::new($n, $f);
            config.set_caesar_wait_condition($wait);
            config
        }};
    }

    fn ci() -> bool {
        if let Ok(value) = std::env::var("CI") {
            // if ci is set, it should be a bool
            let ci =
                value.parse::<bool>().expect("CI env var should be a bool");
            if ci {
                true
            } else {
                panic!("CI env var is set and it's not true");
            }
        } else {
            false
        }
    }

    /// Computes the number of commands per client and clients per process
    /// according to "CI" env var; if set to true, run the tests with a smaller
    /// load
    fn small_load_in_ci() -> (usize, usize) {
        if ci() {
            // 10 commands per client and 1 client per process
            (10, 1)
        } else {
            (COMMANDS_PER_CLIENT, CLIENTS_PER_PROCESS)
        }
    }

    // ---- tempo tests ---- //
    #[test]
    fn sim_tempo_3_1_test() {
        let metrics = sim_test::<TempoSequential>(
            tempo_config!(3, 1),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_real_time_tempo_3_1_test() {
        // NOTE: with n = 3 we don't really need real time clocks to get the
        // best results
        let clock_bump_interval = Duration::from_millis(50);
        let metrics = sim_test::<TempoSequential>(
            tempo_config!(3, 1, clock_bump_interval),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_tempo_5_1_test() {
        let metrics = sim_test::<TempoSequential>(
            tempo_config!(5, 1),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_tempo_5_2_test() {
        let metrics = sim_test::<TempoSequential>(
            tempo_config!(5, 2),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
    }

    #[test]
    fn sim_tempo_5_2_nfr_test() {
        let mut config = tempo_config!(5, 2);
        config.set_nfr(true);
        let read_only_percentage = 20;
        let keys_per_command = 1;
        let metrics = sim_test::<TempoSequential>(
            config,
            read_only_percentage,
            keys_per_command,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
        assert_eq!(metrics.slow_paths_reads(), 0);
    }

    #[test]
    fn sim_real_time_tempo_5_1_test() {
        let clock_bump_interval = Duration::from_millis(50);
        let metrics = sim_test::<TempoSequential>(
            tempo_config!(5, 1, clock_bump_interval),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_3_1_atomic_test() {
        // tempo atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 3;
        let executors = 3;
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_3_1_locked_test() {
        let workers = 3;
        let executors = 3;
        let metrics = run_test::<TempoLocked>(
            tempo_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_real_time_tempo_3_1_atomic_test() {
        let workers = 3;
        let executors = 3;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(3, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_5_1_atomic_test() {
        let workers = 3;
        let executors = 3;
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(5, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_5_2_atomic_test() {
        let workers = 3;
        let executors = 3;
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(5, 2),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
    }

    // ---- tempo (partial replication) tests ---- //
    #[test]
    fn run_tempo_3_1_atomic_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_3_1_atomic_partial_replication_three_shards_test() {
        let shard_count = 3;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_tempo_5_2_atomic_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<TempoAtomic>(
            tempo_config!(5, 2),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert!(metrics.slow_paths() > 0);
    }

    // ---- atlas tests ---- //
    #[test]
    fn sim_atlas_3_1_test() {
        let metrics = sim_test::<AtlasSequential>(
            config!(3, 1),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_atlas_5_1_test() {
        let metrics = sim_test::<AtlasSequential>(
            config!(3, 1),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_atlas_5_2_test() {
        let metrics = sim_test::<AtlasSequential>(
            config!(5, 2),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
    }

    #[test]
    fn sim_atlas_5_2_nfr_test() {
        let mut config = config!(5, 2);
        config.set_nfr(true);
        let read_only_percentage = 20;
        let keys_per_command = 1;
        let metrics = sim_test::<AtlasSequential>(
            config,
            read_only_percentage,
            keys_per_command,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
        assert_eq!(metrics.slow_paths_reads(), 0);
    }

    #[test]
    fn run_atlas_3_1_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let metrics = run_test::<AtlasLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    // ---- atlas (partial replication) tests ---- //
    #[test]
    fn run_atlas_3_1_locked_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<AtlasLocked>(
            tempo_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_atlas_3_1_locked_partial_replication_four_shards_test() {
        let shard_count = 4;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<AtlasLocked>(
            tempo_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn run_atlas_5_2_locked_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let metrics = run_test::<AtlasLocked>(
            tempo_config!(5, 2),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert!(metrics.slow_paths() > 0);
    }

    // ---- epaxos tests ---- //
    #[test]
    fn sim_epaxos_3_1_test() {
        let metrics = sim_test::<EPaxosSequential>(
            config!(3, 1),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    #[test]
    fn sim_epaxos_5_2_test() {
        let metrics = sim_test::<EPaxosSequential>(
            config!(5, 2),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
    }

    #[test]
    fn sim_epaxos_5_2_nfr_test() {
        let mut config = config!(5, 2);
        config.set_nfr(true);
        let read_only_percentage = 20;
        let keys_per_command = 1;
        let metrics = sim_test::<EPaxosSequential>(
            config,
            read_only_percentage,
            keys_per_command,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(metrics.slow_paths() > 0);
        assert_eq!(metrics.slow_paths_reads(), 0);
    }

    #[test]
    fn run_epaxos_3_1_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let metrics = run_test::<EPaxosLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(metrics.slow_paths(), 0);
    }

    // ---- caesar tests ---- //
    #[test]
    fn sim_caesar_wait_3_1_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(3, 1, true),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_3_1_no_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(3, 1, false),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_5_2_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(5, 2, true),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_5_2_no_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(5, 2, false),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_caesar_3_1_wait_locked_test() {
        let workers = 4;
        let executors = 1;
        let _slow_paths = run_test::<CaesarLocked>(
            caesar_config!(3, 1, true),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[ignore]
    #[test]
    fn run_caesar_5_2_wait_locked_test() {
        let workers = 4;
        let executors = 1;
        let _slow_paths = run_test::<CaesarLocked>(
            caesar_config!(5, 2, true),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    // ---- fpaxos tests ---- //
    #[test]
    fn sim_fpaxos_3_1_test() {
        let leader = 1;
        sim_test::<FPaxos>(
            config!(3, 1, leader),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_fpaxos_5_2_test() {
        let leader = 1;
        sim_test::<FPaxos>(
            config!(5, 2, leader),
            READ_ONLY_PERCENTAGE,
            KEYS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_fpaxos_3_1_sequential_test() {
        let leader = 1;
        // run fpaxos in sequential mode
        let workers = 1;
        let executors = 1;
        run_test::<FPaxos>(
            config!(3, 1, leader),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_fpaxos_3_1_parallel_test() {
        let leader = 1;
        // run fpaxos in paralel mode (in terms of workers, since execution is
        // never parallel)
        let workers = 3;
        let executors = 1;
        run_test::<FPaxos>(
            config!(3, 1, leader),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[allow(dead_code)]
    fn metrics_inspect<P>(worker: &P) -> ProtocolMetrics
    where
        P: Protocol,
    {
        worker.metrics().clone()
    }

    fn run_test<P>(
        mut config: Config,
        shard_count: usize,
        workers: usize,
        executors: usize,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> ProtocolMetrics
    where
        P: Protocol + Send + 'static,
    {
        update_config(&mut config, shard_count);

        // create workload
        let payload_size = 1;
        let workload = Workload::new(
            shard_count,
            KEY_GEN,
            KEYS_PER_COMMAND,
            commands_per_client,
            payload_size,
        );

        // run until the clients end + another 10 seconds
        let extra_run_time = Some(Duration::from_secs(10));
        let metrics = tokio_test_runtime()
            .block_on(run_test_with_inspect_fun::<P, ProtocolMetrics>(
                config,
                workload,
                clients_per_process,
                workers,
                executors,
                Some(metrics_inspect),
                extra_run_time,
            ))
            .expect("run should complete successfully")
            .into_iter()
            .map(|(process_id, all_workers_metrics)| {
                // aggregate worker metrics
                let mut process_metrics = ProtocolMetrics::new();
                all_workers_metrics.into_iter().for_each(|worker_metrics| {
                    process_metrics.merge(&worker_metrics);
                });
                (process_id, process_metrics)
            })
            .collect();

        check_metrics(config, commands_per_client, clients_per_process, metrics)
    }

    fn sim_test<P: Protocol>(
        mut config: Config,
        read_only_percentage: usize,
        keys_per_command: usize,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> ProtocolMetrics {
        let shard_count = 1;
        update_config(&mut config, shard_count);

        // planet
        let planet = Planet::new();

        // clients workload
        let payload_size = 1;
        let mut workload = Workload::new(
            shard_count,
            KEY_GEN,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        workload.set_read_only_percentage(read_only_percentage);

        // process and client regions
        let mut regions = planet.regions();
        regions.truncate(config.n());
        let process_regions = regions.clone();
        let client_regions = regions.clone();

        // create runner
        let mut runner: Runner<P> = Runner::new(
            planet,
            config,
            workload,
            clients_per_process,
            process_regions,
            client_regions,
        );

        // reorder network messages
        runner.reorder_messages();

        // run simulation until the clients end + another 10 seconds (for GC)
        let extra_sim_time = Some(Duration::from_secs(10));
        let (metrics, executors_monitors, _) = runner.run(extra_sim_time);

        // fetch slow paths and stable count from metrics
        let metrics = metrics
            .into_iter()
            .map(|(process_id, (process_metrics, _executors_metrics))| {
                (process_id, process_metrics)
            })
            .collect();

        let executors_monitors: Vec<_> = executors_monitors
            .into_iter()
            .map(|(process_id, order)| {
                let order = order
                    .expect("processes should be monitoring execution orders");
                (process_id, order)
            })
            .collect();
        check_monitors(executors_monitors);

        check_metrics(config, commands_per_client, clients_per_process, metrics)
    }

    fn update_config(config: &mut Config, shard_count: usize) {
        // make sure execution order is monitored
        config.set_executor_monitor_execution_order(true);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // make sure executed notification are being sent (which it will affect
        // the protocols that have implemented such functionality)
        config.set_executor_executed_notification_interval(
            Duration::from_millis(100),
        );

        // set number of shards
        config.set_shard_count(shard_count);
    }

    fn check_monitors(
        mut executor_monitors: Vec<(ProcessId, ExecutionOrderMonitor)>,
    ) {
        // take the first monitor and check that all the other are equal
        let (process_a, monitor_a) = executor_monitors
            .pop()
            .expect("there's more than one process in the test");
        for (process_b, monitor_b) in executor_monitors {
            if monitor_a != monitor_b {
                return compute_diff_on_monitors(
                    process_a, monitor_a, process_b, monitor_b,
                );
            }
        }
    }

    fn compute_diff_on_monitors(
        process_a: ProcessId,
        monitor_a: ExecutionOrderMonitor,
        process_b: ProcessId,
        monitor_b: ExecutionOrderMonitor,
    ) {
        assert_eq!(
            monitor_a.len(),
            monitor_b.len(),
            "monitors should have the same number of keys"
        );

        for key in monitor_a.keys() {
            let key_order_a = monitor_a
                .get_order(key)
                .expect("monitors should have the same keys");
            let key_order_b = monitor_b
                .get_order(key)
                .expect("monitors should have the same keys");
            compute_diff_on_key(
                key,
                process_a,
                key_order_a,
                process_b,
                key_order_b,
            );
        }
    }

    fn compute_diff_on_key(
        key: &Key,
        process_a: ProcessId,
        key_order_a: &Vec<Rifl>,
        process_b: ProcessId,
        key_order_b: &Vec<Rifl>,
    ) {
        assert_eq!(
            key_order_a.len(),
            key_order_b.len(),
            "orders per key should have the same number of rifls"
        );
        let len = key_order_a.len();

        if key_order_a != key_order_b {
            let first_different =
                find_different_rifl(key_order_a, key_order_b, 0..len);
            let last_equal = 1 + find_different_rifl(
                key_order_a,
                key_order_b,
                (0..len).rev(),
            );
            let key_order_a = key_order_a[first_different..last_equal].to_vec();
            let key_order_b = key_order_b[first_different..last_equal].to_vec();
            panic!(
                "different execution orders on key {:?}\n   process {:?}: {:?}\n   process {:?}: {:?}",
                key, process_a, key_order_a, process_b, key_order_b,
            )
        }
    }

    fn find_different_rifl(
        key_order_a: &Vec<Rifl>,
        key_order_b: &Vec<Rifl>,
        range: impl Iterator<Item = usize>,
    ) -> usize {
        for i in range {
            if key_order_a[i] != key_order_b[i] {
                return i;
            }
        }
        unreachable!(
            "the execution orders are different, so we must never reach this"
        )
    }

    fn check_metrics(
        config: Config,
        commands_per_client: usize,
        clients_per_process: usize,
        metrics: HashMap<ProcessId, ProtocolMetrics>,
    ) -> ProtocolMetrics {
        let mut all_metrics = ProtocolMetrics::new();
        // check process stats
        metrics
            .into_iter()
            .for_each(|(process_id, process_metrics)| {
                println!(
                    "process id = {} | fast = {} | slow = {} | fast(R) = {} | slow(R) = {} | stable = {}",
                    process_id,
                    process_metrics.fast_paths(),
                    process_metrics.slow_paths(),
                    process_metrics.fast_paths_reads(),
                    process_metrics.slow_paths_reads(),
                    process_metrics.stable()
                );
                all_metrics.merge(&process_metrics);
            });

        // compute the min and max number of MCommit messages:
        // - we have min, if all commmands accesss a single shard
        // - we have max, if all commmands accesss all shards
        let total_processes = config.n() * config.shard_count();
        let total_clients = clients_per_process * total_processes;
        let min_total_commits = commands_per_client * total_clients;
        let max_total_commits = min_total_commits * config.shard_count();

        // check that all commands were committed (only for leaderless
        // protocols)
        if config.leader().is_none() {
            let total_commits =
                (all_metrics.fast_paths() + all_metrics.slow_paths()) as usize;
            assert!(
                total_commits >= min_total_commits
                    && total_commits <= max_total_commits,
                "number of committed commands out of bounds"
            );
        }

        // check GC:
        // - if there's a leader (i.e. FPaxos), GC will only prune commands at
        //   f+1 acceptors
        // - otherwise, GC will prune comands at all processes
        //
        // since GC only happens at the targetted shard, `gc_at` only considers
        // the size of the shard (i.e., no need to multiply by
        // `config.shard_count()`)
        let gc_at = if config.leader().is_some() {
            config.f() + 1
        } else {
            config.n()
        };
        assert_eq!(
            gc_at * min_total_commits,
            all_metrics.stable() as usize,
            "not all processes gced"
        );

        // return all metrics
        all_metrics
    }
}
