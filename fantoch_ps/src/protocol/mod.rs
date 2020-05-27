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

// // This module contains the definition of `Caesar`.
// mod caesar;

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use fpaxos::FPaxos;
pub use newt::{NewtAtomic, NewtSequential};
// pub use caesar::Caesar;

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::Workload;
    use fantoch::config::Config;
    use fantoch::id::ProcessId;
    use fantoch::planet::Planet;
    use fantoch::protocol::{Protocol, ProtocolMetricsKind};
    use fantoch::run::tests::run_test_with_inspect_fun;
    use fantoch::sim::Runner;
    use std::collections::HashMap;

    #[test]
    fn sim_newt_3_1_test() {
        let slow_paths = sim_test::<NewtSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_1_test() {
        let slow_paths = sim_test::<NewtSequential>(5, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_newt_3_1_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 4;
        let slow_paths =
            run_test::<NewtSequential>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_newt_3_1_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths =
            run_test::<NewtAtomic>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_newt_5_1_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 4;
        let slow_paths =
            run_test::<NewtSequential>(5, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_newt_5_1_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths =
            run_test::<NewtAtomic>(5, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    // TODO ignore since the slow path is not yet implemented
    #[ignore]
    #[test]
    fn sim_newt_5_2_test() {
        let slow_paths = sim_test::<NewtSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_atlas_3_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_2_test() {
        let slow_paths = sim_test::<AtlasSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[tokio::test]
    async fn run_atlas_3_1_sequential_test() {
        // atlas sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let slow_paths =
            run_test::<AtlasSequential>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_atlas_3_1_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths =
            run_test::<AtlasLocked>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_epaxos_3_1_test() {
        let slow_paths = sim_test::<EPaxosSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_epaxos_5_2_test() {
        let slow_paths = sim_test::<EPaxosSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[tokio::test]
    async fn run_epaxos_3_1_sequential_test() {
        // epaxos sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let slow_paths =
            run_test::<EPaxosSequential>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[tokio::test]
    async fn run_epaxos_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths =
            run_test::<EPaxosLocked>(3, 1, false, workers, executors).await;
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_fpaxos_3_1_test() {
        sim_test::<FPaxos>(3, 1, true);
    }

    #[test]
    fn sim_fpaxos_5_2_test() {
        sim_test::<FPaxos>(5, 2, true);
    }

    #[tokio::test]
    async fn run_fpaxos_3_1_sequential_test() {
        // run fpaxos in sequential mode
        let workers = 1;
        let executors = 1;
        run_test::<FPaxos>(3, 1, true, workers, executors).await;
    }

    #[tokio::test]
    async fn run_fpaxos_3_1_parallel_test() {
        // run fpaxos in paralel mode (in terms of workers, since execution is
        // never parallel)
        let workers = 3;
        let executors = 1;
        run_test::<FPaxos>(3, 1, true, workers, executors).await;
    }

    // global test config
    const COMMANDS_PER_CLIENT: usize = 100;
    const CLIENTS_PER_REGION: usize = 10;
    const CONFLICT_RATE: usize = 100;

    #[allow(dead_code)]
    fn metrics_inspect<P>(worker: &P) -> (usize, usize)
    where
        P: Protocol,
    {
        let slow_paths = worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::SlowPath)
            .cloned()
            .unwrap_or_default() as usize;
        let stable_count = worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::Stable)
            .cloned()
            .unwrap_or_default() as usize;
        (slow_paths, stable_count)
    }

    async fn run_test<P>(
        n: usize,
        f: usize,
        with_leader: bool,
        workers: usize,
        executors: usize,
    ) -> u64
    where
        P: Protocol + Send + 'static,
    {
        // run until the clients end + another 10 seconds (10000ms)
        let extra_run_time = Some(10_000);
        let tracer_show_interval = None;
        let metrics = run_test_with_inspect_fun::<P, (usize, usize)>(
            n,
            f,
            with_leader,
            workers,
            executors,
            CONFLICT_RATE,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_REGION,
            extra_run_time,
            tracer_show_interval,
            Some(metrics_inspect),
        )
        .await
        .expect("run should complete successfully")
        .into_iter()
        .map(|(process_id, process_metrics)| {
            // aggregate worker metrics
            let mut total_slow_paths = 0;
            let mut total_stable_count = 0;
            process_metrics.into_iter().for_each(
                |(slow_paths, stable_count)| {
                    total_slow_paths += slow_paths;
                    total_stable_count += stable_count;
                },
            );
            (
                process_id,
                (total_slow_paths as u64, total_stable_count as u64),
            )
        })
        .collect();

        check_metrics(n, f, with_leader, metrics)
    }

    fn sim_test<P: Protocol>(n: usize, f: usize, with_leader: bool) -> u64 {
        // planet
        let planet = Planet::new();

        // config
        let mut config = Config::new(n, f);

        // set process 0 as leader if we need one
        if with_leader {
            config.set_leader(1);
        }

        // clients workload
        let payload_size = 1;
        let workload =
            Workload::new(CONFLICT_RATE, COMMANDS_PER_CLIENT, payload_size);

        // process and client regions
        let mut regions = planet.regions();
        regions.truncate(n);
        let process_regions = regions.clone();
        let client_regions = regions.clone();

        // create runner
        let mut runner: Runner<P> = Runner::new(
            planet,
            config,
            workload,
            CLIENTS_PER_REGION,
            process_regions,
            client_regions,
        );

        // run simulation until the clients end + another 2 seconds (2000ms)
        let extra_sim_time = Some(2000);
        let (metrics, _) = runner.run(extra_sim_time);

        // fetch slow paths and stable count from metrics
        let metrics = metrics
            .into_iter()
            .map(|(process_id, process_metrics)| {
                // get slow paths
                let slow_paths = process_metrics
                    .get_aggregated(ProtocolMetricsKind::SlowPath)
                    .cloned()
                    .unwrap_or_default();

                // get stable count
                let stable_count = process_metrics
                    .get_aggregated(ProtocolMetricsKind::Stable)
                    .cloned()
                    .unwrap_or_default();

                (process_id, (slow_paths, stable_count))
            })
            .collect();

        check_metrics(n, f, with_leader, metrics)
    }

    fn check_metrics(
        n: usize,
        f: usize,
        with_leader: bool,
        metrics: HashMap<ProcessId, (u64, u64)>,
    ) -> u64 {
        // total slow path count
        let mut total_slow_paths = 0;

        // check process stats
        let gced = metrics
            .into_iter()
            .filter(|(_, (slow_paths, stable_count))| {
                total_slow_paths += slow_paths;
                // check if this process gc-ed all commands
                *stable_count
                    == (COMMANDS_PER_CLIENT * CLIENTS_PER_REGION * n) as u64
            })
            .count();

        // check if the correct number of processed gc-ed:
        // - if there's a leader (i.e. FPaxos), GC will only prune commands at
        //   f+1 acceptors
        // - otherwise, GC will prune comands at all processes
        let expected = if with_leader { f + 1 } else { n };
        assert_eq!(gced, expected);

        // return number of slow paths
        total_slow_paths
    }
}
