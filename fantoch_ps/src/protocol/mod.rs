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
    use fantoch::planet::{Planet, Region};
    use fantoch::protocol::{Protocol, ProtocolMetricsKind};
    use fantoch::run::tests::run_test;
    use fantoch::sim::Runner;

    #[test]
    fn sim_newt_test() {
        sim_gc_test::<NewtSequential>()
    }

    #[test]
    fn sim_atlas_test() {
        sim_gc_test::<AtlasSequential>()
    }

    #[test]
    fn sim_epaxos_test() {
        sim_gc_test::<EPaxosSequential>()
    }

    #[test]
    fn sim_fpaxos_test() {
        sim_gc_test::<FPaxos>()
    }

    #[tokio::test]
    async fn run_newt_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 2;
        let with_leader = false;
        run_test::<NewtSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_newt_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<NewtAtomic>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_atlas_sequential_test() {
        // atlas sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let with_leader = false;
        run_test::<AtlasSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_atlas_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<AtlasLocked>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_epaxos_sequential_test() {
        // epaxos sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let with_leader = false;
        run_test::<EPaxosSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_epaxos_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<EPaxosLocked>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_fpaxos_sequential_test() {
        // run fpaxos in sequential mode
        let workers = 1;
        let executors = 1;
        let with_leader = true;
        run_test::<FPaxos>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_fpaxos_parallel_test() {
        // run fpaxos in paralel mode (in terms of workers, since execution is
        // never parallel)
        let workers = 3;
        let executors = 1;
        let with_leader = true;
        run_test::<FPaxos>(workers, executors, with_leader).await
    }

    fn sim_gc_test<P: Protocol>() {
        // planet
        let planet = Planet::new();

        // config
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // clients workload
        let conflict_rate = 100;
        let total_commands = 1000;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);
        let clients_per_region = 3;

        // process regions
        let process_regions = vec![
            Region::new("asia-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
        ];

        // client regions
        let client_regions = vec![
            Region::new("us-west1"),
            Region::new("us-west2"),
            Region::new("europe-north1"),
        ];

        // create runner
        let mut runner: Runner<P> = Runner::new(
            planet,
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );

        // run simulation until the clients end + another second (1000ms)
        let (processes_metrics, _) = runner.run(Some(1000));

        // check process stats
        let all_gced = processes_metrics.values().into_iter().all(|metrics| {
            // check stability has run
            let stable_count = metrics
                .get_aggregated(ProtocolMetricsKind::Stable)
                .expect("stability should have happened");

            // check that all commands were gc-ed:
            let expected = (total_commands * clients_per_region * n) as u64;
            *stable_count == expected
        });
        assert!(all_gced)
    }
}
