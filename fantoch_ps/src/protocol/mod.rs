// This module contains common data-structures between protocols.
pub mod common;

// This module contains the definition of `Atlas`.
mod atlas;

// This module contains the definition of `EPaxos`.
mod epaxos;

// This module contains the definition of `Newt`.
mod newt;

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
pub use newt::{NewtAtomic, NewtLocked, NewtSequential};

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
    const COMMANDS_PER_CLIENT: usize = 100;
    const KEY_GEN: KeyGen = KeyGen::ConflictPool {
        conflict_rate: 50,
        pool_size: 1,
    };
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

    macro_rules! newt_config {
        ($n:expr, $f:expr) => {{
            let mut config = Config::new($n, $f);
            // always set `newt_detached_send_interval`
            config.set_newt_detached_send_interval(Duration::from_millis(100));
            config
        }};
        ($n:expr, $f:expr, $clock_bump_interval:expr) => {{
            let mut config = newt_config!($n, $f);
            config.set_newt_tiny_quorums(true);
            config.set_newt_clock_bump_interval($clock_bump_interval);
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

    // ---- newt tests ---- //
    #[test]
    fn sim_newt_3_1_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_real_time_newt_3_1_test() {
        // NOTE: with n = 3 we don't really need real time clocks to get the
        // best results
        let clock_bump_interval = Duration::from_millis(50);
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(3, 1, clock_bump_interval),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_1_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_2_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_real_time_newt_5_1_test() {
        let clock_bump_interval = Duration::from_millis(50);
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 1, clock_bump_interval),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 3;
        let executors = 3;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_locked_test() {
        let workers = 3;
        let executors = 3;
        let slow_paths = run_test::<NewtLocked>(
            newt_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_real_time_newt_3_1_atomic_test() {
        let workers = 3;
        let executors = 3;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_1_atomic_test() {
        let workers = 3;
        let executors = 3;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_2_atomic_test() {
        let workers = 3;
        let executors = 3;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 2),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    // ---- newt (partial replication) tests ---- //
    #[test]
    fn run_newt_3_1_atomic_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_atomic_partial_replication_three_shards_test() {
        let shard_count = 3;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_2_atomic_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 2),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert!(slow_paths > 0);
    }

    // ---- atlas tests ---- //
    #[test]
    fn sim_atlas_3_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_2_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn run_atlas_3_1_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<AtlasLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    // ---- atlas (partial replication) tests ---- //
    #[test]
    fn run_atlas_3_1_locked_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_atlas_3_1_locked_partial_replication_four_shards_test() {
        let shard_count = 4;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_atlas_5_2_locked_partial_replication_two_shards_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2; // atlas executor can be parallel in partial replication
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(5, 2),
            shard_count,
            workers,
            executors,
            commands_per_client,
            clients_per_process,
        );
        assert!(slow_paths > 0);
    }

    // ---- epaxos tests ---- //
    #[test]
    fn sim_epaxos_3_1_test() {
        let slow_paths = sim_test::<EPaxosSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_epaxos_5_2_test() {
        let slow_paths = sim_test::<EPaxosSequential>(
            config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn run_epaxos_3_1_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<EPaxosLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    // ---- caesar tests ---- //
    #[test]
    fn sim_caesar_wait_3_1_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(3, 1, true),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_3_1_no_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(3, 1, false),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_5_2_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(5, 2, true),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_caesar_5_2_no_wait_test() {
        let _slow_paths = sim_test::<CaesarLocked>(
            caesar_config!(5, 2, false),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_caesar_3_1_wait_locked_test() {
        let workers = 3;
        let executors = 3;
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
        let workers = 3;
        let executors = 3;
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
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_fpaxos_5_2_test() {
        let leader = 1;
        sim_test::<FPaxos>(
            config!(5, 2, leader),
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
    fn metrics_inspect<P>(worker: &P) -> (usize, usize, usize)
    where
        P: Protocol,
    {
        extract_process_metrics(worker.metrics())
    }

    fn extract_process_metrics(
        metrics: &ProtocolMetrics,
    ) -> (usize, usize, usize) {
        let metric = |kind| {
            metrics.get_aggregated(kind).cloned().unwrap_or_default() as usize
        };
        let fast_paths = metric(ProtocolMetricsKind::FastPath);
        let slow_paths = metric(ProtocolMetricsKind::SlowPath);
        let stable_count = metric(ProtocolMetricsKind::Stable);
        (fast_paths, slow_paths, stable_count)
    }

    fn run_test<P>(
        mut config: Config,
        shard_count: usize,
        workers: usize,
        executors: usize,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> usize
    where
        P: Protocol + Send + 'static,
    {
        update_config(&mut config, shard_count);

        // create workload
        let keys_per_command = 2;
        let payload_size = 1;
        let workload = Workload::new(
            shard_count,
            KEY_GEN,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // run until the clients end + another 10 seconds
        let extra_run_time = Some(Duration::from_secs(10));
        let metrics = tokio_test_runtime()
            .block_on(run_test_with_inspect_fun::<P, (usize, usize, usize)>(
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
            .map(|(process_id, process_metrics)| {
                // aggregate worker metrics
                let mut total_fast_paths = 0;
                let mut total_slow_paths = 0;
                let mut total_stable_count = 0;
                process_metrics.into_iter().for_each(
                    |(fast_paths, slow_paths, stable_count)| {
                        total_fast_paths += fast_paths;
                        total_slow_paths += slow_paths;
                        total_stable_count += stable_count;
                    },
                );
                (
                    process_id,
                    (total_fast_paths, total_slow_paths, total_stable_count),
                )
            })
            .collect();

        check_metrics(config, commands_per_client, clients_per_process, metrics)
    }

    fn sim_test<P: Protocol>(
        mut config: Config,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> usize {
        let shard_count = 1;
        update_config(&mut config, shard_count);

        // planet
        let planet = Planet::new();

        // clients workload
        let keys_per_command = 2;
        let payload_size = 1;
        let workload = Workload::new(
            shard_count,
            KEY_GEN,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

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
                let (fast_paths, slow_paths, stable_count) =
                    extract_process_metrics(&process_metrics);
                (process_id, (fast_paths, slow_paths, stable_count))
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
        metrics: HashMap<ProcessId, (usize, usize, usize)>,
    ) -> usize {
        // total fast and slow paths count
        let mut total_fast_paths = 0;
        let mut total_slow_paths = 0;
        let mut total_stable = 0;

        // check process stats
        metrics.into_iter().for_each(
            |(process_id, (fast_paths, slow_paths, stable))| {
                println!(
                    "process id = {} | fast = {} | slow = {} | stable = {}",
                    process_id, fast_paths, slow_paths, stable
                );
                total_fast_paths += fast_paths;
                total_slow_paths += slow_paths;
                total_stable += stable;
            },
        );

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
            let total_commits = total_fast_paths + total_slow_paths;
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
            total_stable,
            "not all processes gced"
        );

        // return number of slow paths
        total_slow_paths
    }
}
