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

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use fpaxos::FPaxos;
pub use newt::{NewtAtomic, NewtSequential};


#[cfg(test)]
mod tests {
    use fantoch::run::tests::run_test;
    use super::*;

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
}