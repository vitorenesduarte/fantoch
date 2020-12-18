use crate::id::ProcessId;
use crate::warn;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
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
    // starting leader process
    leader: Option<ProcessId>,
    /// defines whether newt should employ tiny quorums or not
    newt_tiny_quorums: bool,
    /// defines the interval between clock bumps, if any
    newt_clock_bump_interval: Option<Duration>,
    /// defines the interval the sending of `MDetached` messages in newt, if
    /// any
    newt_detached_send_interval: Option<Duration>,
    /// defines whether protocols should try to bypass the fast quorum process
    /// ack (which is only possible if the fast quorum size is 2)
    skip_fast_ack: bool,
}

impl Config {
    /// Create a new `Config`.
    /// The first argument `n` represents the number of processes in the system.
    /// The second argument `f` represents the number of faults tolerated by the
    /// system.
    pub fn new(n: usize, f: usize) -> Self {
        if f > n / 2 {
            warn!("f={} is larger than a minority with n={}", f, n);
        }
        // by default, `shard_count = 1`
        let shard_count = 1;
        // by default, execution is not skipped
        let execute_at_commit = false;
        // by default, executor cleanups happen every 5ms
        let executor_cleanup_interval = Duration::from_millis(5);
        // by default, executed notifications happen every 5ms
        let executor_executed_notification_interval = Duration::from_millis(5);
        // by default, pending commnads are not monitored
        let executor_monitor_pending_interval = None;
        // by default, executors do not monitor execution order
        let executor_monitor_execution_order = false;
        // by default, commands are deleted at commit time
        let gc_interval = None;
        // by default, there's no leader
        let leader = None;
        // by default, `newt_tiny_quorums = false`
        let newt_tiny_quorums = false;
        // by default, clocks are not bumped periodically
        let newt_clock_bump_interval = None;
        // by default, `MDetached` messages are not sent
        let newt_detached_send_interval = None;
        // by default `skip_fast_ack = false;
        let skip_fast_ack = false;
        Self {
            n,
            f,
            shard_count,
            execute_at_commit,
            executor_cleanup_interval,
            executor_executed_notification_interval,
            executor_monitor_pending_interval,
            executor_monitor_execution_order,
            gc_interval,
            leader,
            newt_tiny_quorums,
            newt_clock_bump_interval,
            newt_detached_send_interval,
            skip_fast_ack,
        }
    }

    /// Retrieve the number of processes.
    pub fn n(&self) -> usize {
        self.n
    }

    /// Retrieve the number of faults tolerated.
    pub fn f(&self) -> usize {
        self.f
    }

    /// Retrieve the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Changes the number of sahrds.
    pub fn set_shard_count(&mut self, shard_count: usize) {
        assert!(shard_count >= 1);
        self.shard_count = shard_count;
    }

    /// Checks whether execution is to be skipped.
    pub fn execute_at_commit(&self) -> bool {
        self.execute_at_commit
    }

    /// Changes the value of `execute_at_commit`.
    pub fn set_execute_at_commit(&mut self, execute_at_commit: bool) {
        self.execute_at_commit = execute_at_commit;
    }

    /// Checks the executor cleanup interval.
    pub fn executor_cleanup_interval(&self) -> Duration {
        self.executor_cleanup_interval
    }

    /// Sets the executor cleanup interval.
    pub fn set_executor_cleanup_interval(&mut self, interval: Duration) {
        self.executor_cleanup_interval = interval;
    }

    /// Checks the executor monitor pending interval.
    pub fn executor_monitor_pending_interval(&self) -> Option<Duration> {
        self.executor_monitor_pending_interval
    }

    /// Sets the executor monitor pending interval.
    pub fn set_executor_monitor_pending_interval(
        &mut self,
        interval: Duration,
    ) {
        self.executor_monitor_pending_interval = Some(interval);
    }

    /// Checks the whether executors should monitor execution order.
    pub fn executor_monitor_execution_order(&self) -> bool {
        self.executor_monitor_execution_order
    }

    /// Sets the executor monitor execution order.
    pub fn set_executor_monitor_execution_order(
        &mut self,
        executor_monitor_execution_order: bool,
    ) {
        self.executor_monitor_execution_order =
            executor_monitor_execution_order;
    }

    /// Checks the executed notification interval.
    pub fn executor_executed_notification_interval(&self) -> Duration {
        self.executor_executed_notification_interval
    }

    /// Sets the executed notification interval.
    pub fn set_executor_executed_notification_interval(
        &mut self,
        interval: Duration,
    ) {
        self.executor_executed_notification_interval = interval;
    }

    /// Checks the garbage collection interval.
    pub fn gc_interval(&self) -> Option<Duration> {
        self.gc_interval
    }

    /// Sets the garbage collection interval.
    pub fn set_gc_interval(&mut self, interval: Duration) {
        self.gc_interval = Some(interval);
    }

    /// Checks whether a starting leader has been defined.
    pub fn leader(&self) -> Option<ProcessId> {
        self.leader
    }

    /// Sets the starting leader.
    pub fn set_leader(&mut self, leader: ProcessId) {
        self.leader = Some(leader);
    }

    /// Checks whether newt tiny quorums is enabled or not.
    pub fn newt_tiny_quorums(&self) -> bool {
        self.newt_tiny_quorums
    }

    /// Changes the value of `newt_tiny_quorums`.
    pub fn set_newt_tiny_quorums(&mut self, newt_tiny_quorums: bool) {
        self.newt_tiny_quorums = newt_tiny_quorums;
    }

    /// Checks newt clock bump interval.
    pub fn newt_clock_bump_interval(&self) -> Option<Duration> {
        self.newt_clock_bump_interval
    }

    /// Sets newt clock bump interval.
    pub fn set_newt_clock_bump_interval(&mut self, interval: Duration) {
        self.newt_clock_bump_interval = Some(interval);
    }

    /// Checks newt
    pub fn newt_detached_send_interval(&self) -> Option<Duration> {
        self.newt_detached_send_interval
    }

    /// Sets newt clock bump interval.
    pub fn set_newt_detached_send_interval(&mut self, interval: Duration) {
        self.newt_detached_send_interval = Some(interval);
    }

    /// Checks whether skip fast ack is enabled or not.
    pub fn skip_fast_ack(&self) -> bool {
        self.skip_fast_ack
    }

    /// Changes the value of `skip_fast_ack`.
    pub fn set_skip_fast_ack(&mut self, skip_fast_ack: bool) {
        self.skip_fast_ack = skip_fast_ack;
    }
}

impl Config {
    /// Computes `Basic` quorum size.
    pub fn basic_quorum_size(&self) -> usize {
        self.f + 1
    }

    /// Computes `FPaxos` quorum size.
    pub fn fpaxos_quorum_size(&self) -> usize {
        self.f + 1
    }

    /// Computes `Atlas` fast and write quorum sizes.
    pub fn atlas_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = (n / 2) + f;
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `EPaxos` fast and write quorum sizes.
    pub fn epaxos_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        // ignore config.f() since EPaxos always tolerates a minority of
        // failures
        let f = n / 2;
        let fast_quorum_size = f + ((f + 1) / 2 as usize);
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `Caesar` fast and write quorum sizes.
    pub fn caesar_quorum_sizes(&self) -> (usize, usize) {
        let n = self.n;
        let fast_quorum_size = ((3 * n) / 4) + 1;
        let write_quorum_size = (n / 2) + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Computes `Newt` fast quorum size, stability threshold and write quorum
    /// size.
    ///
    /// The threshold should be n - q + 1, where n is the number of processes
    /// and q the size of the quorum used to compute clocks. In `Newt` e.g.
    /// with tiny quorums, although the fast quorum is 2f (which would
    /// suggest q = 2f), in fact q = f + 1. The quorum size of 2f ensures that
    /// all clocks are computed from f + 1 processes. So, n - q + 1 = n - (f
    /// + 1) + 1 = n - f.
    ///
    /// In general, the stability threshold is given by:
    ///   "n - (fast_quorum_size - f + 1) + 1 = n - fast_quorum_size + f"
    /// - this ensures that the stability threshold plus the minimum number of
    ///   processes where clocks are computed (i.e. fast_quorum_size - f + 1) is
    ///   greater than n
    pub fn newt_quorum_sizes(&self) -> (usize, usize, usize) {
        let n = self.n;
        let f = self.f;
        let minority = n / 2;
        let (fast_quorum_size, stability_threshold) = if self.newt_tiny_quorums
        {
            (2 * f, n - f)
        } else {
            (minority + f, minority + 1)
        };
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size, stability_threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config() {
        // n and f
        let n = 5;
        let f = 1;

        // config
        let mut config = Config::new(n, f);

        assert_eq!(config.n(), n);
        assert_eq!(config.f(), f);

        // by default, the number shards is 1.
        assert_eq!(config.shard_count(), 1);

        // but that can change
        let shards = 10;
        config.set_shard_count(shards);
        assert_eq!(config.shard_count(), shards);

        // by deafult, execute at commit is false
        assert!(!config.execute_at_commit());
        // but that can change
        config.set_execute_at_commit(true);
        assert!(config.execute_at_commit());

        // by default, the executor cleanup interval is 5ms
        assert_eq!(
            config.executor_cleanup_interval(),
            Duration::from_millis(5)
        );

        // change its value and check it has changed
        let interval = Duration::from_secs(2);
        config.set_executor_cleanup_interval(interval);
        assert_eq!(config.executor_cleanup_interval(), interval);

        // by default, the executor executed notification interval is 5ms
        assert_eq!(
            config.executor_executed_notification_interval(),
            Duration::from_millis(5)
        );

        // change its value and check it has changed
        let interval = Duration::from_secs(10);
        config.set_executor_executed_notification_interval(interval);
        assert_eq!(config.executor_executed_notification_interval(), interval);

        // by default, there's executor monitor pending interval
        assert_eq!(config.executor_monitor_pending_interval(), None);

        // change its value and check it has changed
        let interval = Duration::from_millis(1);
        config.set_executor_monitor_pending_interval(interval);
        assert_eq!(config.executor_monitor_pending_interval(), Some(interval));

        // by default, executor monitor execution order is false
        assert_eq!(config.executor_monitor_execution_order(), false);
        // but that can change
        config.set_executor_monitor_execution_order(true);
        assert_eq!(config.executor_monitor_execution_order(), true);

        // by default, there's no garbage collection interval
        assert_eq!(config.gc_interval(), None);

        // change its value and check it has changed
        let interval = Duration::from_millis(1);
        config.set_gc_interval(interval);
        assert_eq!(config.gc_interval(), Some(interval));

        // by default, there's no leader
        assert!(config.leader().is_none());
        // but that can change
        let leader = 1;
        config.set_leader(leader);
        assert_eq!(config.leader(), Some(leader));

        // by default, newt tiny quorums is false
        assert!(!config.newt_tiny_quorums());

        // if we change it to false, remains false
        config.set_newt_tiny_quorums(false);
        assert!(!config.newt_tiny_quorums());

        // if we change it to true, it becomes true
        config.set_newt_tiny_quorums(true);
        assert!(config.newt_tiny_quorums());

        // by default, there's no clock bump interval
        assert!(config.newt_clock_bump_interval().is_none());
        // but that can change
        let interval = Duration::from_millis(1);
        config.set_newt_clock_bump_interval(interval);
        assert_eq!(config.newt_clock_bump_interval(), Some(interval));

        // by default, there's no sending of `MDetached` messages
        assert!(config.newt_detached_send_interval().is_none());
        // but that can change
        let interval = Duration::from_millis(2);
        config.set_newt_detached_send_interval(interval);
        assert_eq!(config.newt_detached_send_interval(), Some(interval));

        // by default, skip fast ack is false
        assert!(!config.skip_fast_ack());

        // if we change it to false, remains false
        config.set_skip_fast_ack(false);
        assert!(!config.skip_fast_ack());

        // if we change it to true, it becomes true
        config.set_skip_fast_ack(true);
        assert!(config.skip_fast_ack());
    }

    #[test]
    fn basic_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(config.basic_quorum_size(), 2);

        let config = Config::new(7, 2);
        assert_eq!(config.basic_quorum_size(), 3);

        let config = Config::new(7, 3);
        assert_eq!(config.basic_quorum_size(), 4);
    }

    #[test]
    fn atlas_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(config.atlas_quorum_sizes(), (4, 2));

        let config = Config::new(7, 2);
        assert_eq!(config.atlas_quorum_sizes(), (5, 3));

        let config = Config::new(7, 3);
        assert_eq!(config.atlas_quorum_sizes(), (6, 4));
    }

    #[test]
    fn epaxos_parameters() {
        let ns = vec![3, 5, 7, 9, 11, 13, 15, 17];
        // expected pairs of fast and write quorum sizes
        let expected = vec![
            (2, 2),
            (3, 3),
            (5, 4),
            (6, 5),
            (8, 6),
            (9, 7),
            (11, 8),
            (12, 9),
        ];

        let fs: Vec<_> = ns
            .into_iter()
            .map(|n| {
                // this f value won't be used
                let f = 0;
                let config = Config::new(n, f);
                config.epaxos_quorum_sizes()
            })
            .collect();
        assert_eq!(fs, expected);
    }

    #[test]
    fn caesar_parameters() {
        let ns = vec![3, 5, 7, 9, 11];
        // expected pairs of fast and write quorum sizes
        let expected = vec![(3, 2), (4, 3), (6, 4), (7, 5), (9, 6)];

        let fs: Vec<_> = ns
            .into_iter()
            .map(|n| {
                // this f value won't be used
                let f = 0;
                let config = Config::new(n, f);
                config.caesar_quorum_sizes()
            })
            .collect();
        assert_eq!(fs, expected);
    }

    #[test]
    fn newt_parameters() {
        // tiny quorums = false
        let mut config = Config::new(7, 1);
        config.set_newt_tiny_quorums(false);
        assert_eq!(config.newt_quorum_sizes(), (4, 2, 4));

        let mut config = Config::new(7, 2);
        config.set_newt_tiny_quorums(false);
        assert_eq!(config.newt_quorum_sizes(), (5, 3, 4));

        // tiny quorums = true
        let mut config = Config::new(7, 1);
        config.set_newt_tiny_quorums(true);
        assert_eq!(config.newt_quorum_sizes(), (2, 2, 6));

        let mut config = Config::new(7, 2);
        config.set_newt_tiny_quorums(true);
        assert_eq!(config.newt_quorum_sizes(), (4, 3, 5));
    }
}
