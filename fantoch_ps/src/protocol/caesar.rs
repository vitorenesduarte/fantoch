use crate::executor::GraphExecutor;
use crate::protocol::common::pred::{
    Clock, KeyClocks, QuorumClocks, QuorumRetries, SequentialKeyClocks,
};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{
    Action, BaseProcess, GCTrack, Info, MessageIndex, Protocol,
    ProtocolMetrics, SequentialCommandsInfo,
};
use fantoch::time::SysTime;
use fantoch::{singleton, trace};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use threshold::VClock;

// TODO: Sequential -> Locked
type LockedKeyClocks = SequentialKeyClocks;
pub type CaesarSequential = Caesar<SequentialKeyClocks>;
pub type CaesarLocked = Caesar<LockedKeyClocks>;

type ExecutionInfo = <GraphExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caesar<KC: KeyClocks> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: SequentialCommandsInfo<CaesarInfo>,
    gc_track: GCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
    // commit notifications that arrived before the initial `MPropose` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, Clock, HashSet<Dot>)>,
}

impl<KC: KeyClocks> Protocol for Caesar<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = GraphExecutor;

    /// Creates a new `Caesar` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) =
            config.caesar_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_clocks = KC::new(process_id, shard_id);
        let f = Self::allowed_faults(config.n());
        let cmds = SequentialCommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            f,
            fast_quorum_size,
            write_quorum_size,
        );
        let gc_track = GCTrack::new(process_id, shard_id, config.n());
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let buffered_commits = HashMap::new();

        // create `Caesar`
        let protocol = Self {
            bp,
            key_clocks,
            cmds,
            gc_track,
            to_processes,
            to_executors,
            buffered_commits,
        };

        // create periodic events
        let events = if let Some(interval) = config.gc_interval() {
            vec![(PeriodicEvent::GarbageCollection, interval)]
        } else {
            vec![]
        };

        // return both
        (protocol, events)
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Returns the shard identifier.
    fn shard_id(&self) -> ShardId {
        self.bp.shard_id
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    fn discover(
        &mut self,
        processes: Vec<(ProcessId, ShardId)>,
    ) -> (bool, HashMap<ShardId, ProcessId>) {
        let connect_ok = self.bp.discover(processes);
        (connect_ok, self.bp.closest_shard_process().clone())
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, dot: Option<Dot>, cmd: Command, _time: &dyn SysTime) {
        self.handle_submit(dot, cmd);
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        _from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) {
        match msg {
            Message::MPropose {
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mpropose(from, dot, cmd, quorum, clock, time),
            Message::MProposeAck {
                dot,
                clock,
                deps,
                ok,
            } => self.handle_mproposeack(from, dot, clock, deps, ok, time),
            Message::MCommit { dot, clock, deps } => {
                self.handle_mcommit(from, dot, clock, deps, time)
            }
            Message::MRetry { dot, clock, deps } => {
                self.handle_mretry(from, dot, clock, deps, time)
            }
            Message::MRetryAck { dot, deps } => {
                self.handle_mretryack(from, dot, deps, time)
            }
            Message::MCommitDot { dot } => {
                self.handle_mcommit_dot(from, dot, time)
            }
            Message::MGarbageCollection { committed } => {
                self.handle_mgc(from, committed, time)
            }
            Message::MStable { stable } => {
                self.handle_mstable(from, stable, time)
            }
        }
    }

    /// Handles periodic local events.
    fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime) {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
        }
    }

    /// Returns a new action to be sent to other processes.
    fn to_processes(&mut self) -> Option<Action<Self>> {
        self.to_processes.pop()
    }

    /// Returns new execution info for executors.
    fn to_executors(&mut self) -> Option<ExecutionInfo> {
        self.to_executors.pop()
    }

    fn parallel() -> bool {
        KC::parallel()
    }

    fn leaderless() -> bool {
        true
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl<KC: KeyClocks> Caesar<KC> {
    /// Caesar always tolerates a minority of faults.
    pub fn allowed_faults(n: usize) -> usize {
        n / 2
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // compute its clock
        let clock = self.key_clocks.clock_next();

        // create `MPropose` and target
        let mpropose = Message::MPropose {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.all();

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mpropose,
        });
    }

    fn handle_mpropose(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        remote_clock: Clock,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MPropose({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from,
            time.micros()
        );

        // merge clocks
        self.key_clocks.clock_join(&remote_clock);

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return;
        }

        // check if part of fast quorum
        if !quorum.contains(&self.bp.process_id) {
            // if not:
            // - simply save the payload and set status to `PAYLOAD`
            // - if we received the `MCommit` before the `MPropose`, handle the
            //   `MCommit` now

            info.status = Status::PAYLOAD;
            info.cmd = Some(cmd);

            // check if there's a buffered commit notification; if yes, handle
            // the commit again (since now we have the payload)
            if let Some((from, clock, deps)) =
                self.buffered_commits.remove(&dot)
            {
                self.handle_mcommit(from, dot, clock, deps, time);
            }
            return;
        }

        // compute set of predecessors
        let mut blocking = HashSet::new();
        let deps = self.key_clocks.predecessors(
            &cmd,
            remote_clock,
            Some(&mut blocking),
        );

        // update command info
        info.status = Status::PROPOSE;
        info.cmd = Some(cmd);
        info.deps = deps;
        Self::update_clock(&mut self.key_clocks, dot, info, remote_clock);

        // we send an ok if no command is blocking this command
        // TODO: add wait
        let ok = blocking.is_empty();

        // compute clock to send in the ack
        let (clock, deps) = if ok {
            (info.clock, info.deps.clone())
        } else {
            info.status = Status::REJECT;
            // compute new timestamp for the command
            let new_clock = self.key_clocks.clock_next();
            // compute new set of predecessors for the command
            let cmd = info.cmd.as_ref().expect("command has been set");
            let blocking = None;
            let new_deps =
                self.key_clocks.predecessors(cmd, new_clock, blocking);
            (new_clock, new_deps)
        };

        // create `MProposeAck` and target
        let mproposeack = Message::MProposeAck {
            dot,
            clock,
            deps,
            ok,
        };
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mproposeack,
        });
    }

    fn handle_mproposeack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MProposeAck({:?}, {:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            deps,
            ok,
            from,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // do nothing if we're no longer PROPOSE
        if info.status != Status::PROPOSE {
            return;
        }

        // update quorum deps
        info.quorum_clocks.add(from, clock, deps, ok);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // if yes, get the aggregated results
            let (aggregated_clock, aggregated_deps, aggregated_ok) =
                info.quorum_clocks.aggregated();

            // fast path condition: all reported deps were equal
            if aggregated_ok {
                self.bp.fast_path();
                // fast path: create `MCommit`
                let mcommit = Message::MCommit {
                    dot,
                    clock: aggregated_clock,
                    deps: aggregated_deps,
                };
                let target = self.bp.all();

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mcommit,
                });
            } else {
                self.bp.slow_path();
                // slow path: create `MRetry`
                let mconsensus = Message::MRetry {
                    dot,
                    clock: aggregated_clock,
                    deps: aggregated_deps,
                };
                let target = self.bp.write_quorum();

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mconsensus,
                });
            }
        }
    }

    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MCommit({:?}, {:?}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            deps,
            _time.micros()
        );

        // merge clocks
        self.key_clocks.clock_join(&clock);

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::START {
            // TODO we missed the `MPropose` message and should try to recover
            // the payload:
            // - save this notification just in case we've received the
            //   `MPropose` and `MCommit` in opposite orders (due to
            //   multiplexing)
            self.buffered_commits.insert(dot, (from, clock, deps));
            return;
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return;
        }

        // TODO: create execution info
        // let cmd = info.cmd.clone().expect("there should be a command
        // payload"); let execution_info = ExecutionInfo::add(dot, cmd,
        // deps.clone()); self.to_executors.push(execution_info);

        // update command info:
        info.status = Status::COMMIT;
        info.deps = deps;
        Self::update_clock(&mut self.key_clocks, dot, info, clock);

        if self.gc_running() {
            // notify self with the committed dot
            self.to_processes.push(Action::ToForward {
                msg: Message::MCommitDot { dot },
            });
        } else {
            // if we're not running gc, remove the dot info now
            self.cmds.gc_single(dot);
        }
    }

    fn handle_mretry(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MRetry({:?}, {:?}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            deps,
            _time.micros()
        );

        // merge clocks
        self.key_clocks.clock_join(&clock);

        // get cmd info
        let info = self.cmds.get(dot);

        if matches!(info.status, Status::START | Status::COMMIT) {
            // do nothing if we don't have the payload or if have already
            // committed
            return;
        }

        // update command info:
        info.status = Status::ACCEPT;
        info.deps = deps.clone();
        Self::update_clock(&mut self.key_clocks, dot, info, clock);

        // compute new set of predecessors for the command
        let cmd = info.cmd.as_ref().expect("command has been set");
        let blocking = None;
        let mut new_deps = self.key_clocks.predecessors(cmd, clock, blocking);

        // aggregate with incoming deps
        new_deps.extend(deps);

        // create message and target
        let msg = Message::MRetryAck {
            dot,
            deps: new_deps,
        };
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend { target, msg });
    }

    fn handle_mretryack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        deps: HashSet<Dot>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MRetryAck({:?}, {}) | time={}",
            self.id(),
            dot,
            deps,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if have already committed
            return;
        }

        // update quorum retries
        info.quorum_retries.add(from, deps);

        // check if we have all necessary replies
        if info.quorum_retries.all() {
            // if yes, get the aggregated results
            let aggregated_deps = info.quorum_retries.aggregated();

            // create message and target
            let mcommit = Message::MCommit {
                dot,
                clock: info.clock,
                deps: aggregated_deps,
            };
            let target = self.bp.all();

            // save new action
            self.to_processes.push(Action::ToSend {
                target,
                msg: mcommit,
            });
        }
    }

    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MCommitDot({:?}) | time={}",
            self.id(),
            dot,
            _time.micros()
        );
        assert_eq!(from, self.bp.process_id);
        self.gc_track.commit(dot);
    }

    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            _time.micros()
        );
        self.gc_track.committed_by(from, committed);
        // compute newly stable dots
        let stable = self.gc_track.stable();
        // create `ToForward` to self
        if !stable.is_empty() {
            self.to_processes.push(Action::ToForward {
                msg: Message::MStable { stable },
            });
        }
    }

    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MStable({:?}) from {} | time={}",
            self.id(),
            stable,
            from,
            _time.micros()
        );
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
    }

    fn handle_event_garbage_collection(&mut self, _time: &dyn SysTime) {
        trace!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.micros()
        );

        // retrieve the committed clock
        let committed = self.gc_track.committed();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }

    fn update_clock(
        key_clocks: &mut KC,
        dot: Dot,
        info: &mut CaesarInfo,
        new_clock: Clock,
    ) {
        // first get the command
        let cmd = info.cmd.as_ref().expect("command has been set");

        // remove previous clock from key clocks if we added it before
        let added_before = !info.clock.is_zero();
        if added_before {
            key_clocks.remove(&cmd, info.clock);
        }

        // add new clock to key clocks
        key_clocks.add(dot, &cmd, new_clock);

        // finally update the clock
        info.clock = new_clock;
    }
}

// `CaesarInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone, PartialEq, Eq)]
struct CaesarInfo {
    status: Status,
    // `None` if not set yet
    cmd: Option<Command>,
    clock: Clock,
    deps: HashSet<Dot>,
    // `quorum_clocks` is used by the coordinator to aggregate fast-quorum
    // replies and make the fast-path decision
    quorum_clocks: QuorumClocks,
    // `quorum_retries` is used by the coordinator to aggregate dependencies
    // reported in `MRetry` messages
    quorum_retries: QuorumRetries,
}

impl Info for CaesarInfo {
    fn new(
        process_id: ProcessId,
        _shard_id: ShardId,
        _n: usize,
        _f: usize,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self {
        Self {
            status: Status::START,
            cmd: None,
            clock: Clock::new(process_id),
            deps: HashSet::new(),
            quorum_clocks: QuorumClocks::new(
                process_id,
                fast_quorum_size,
                write_quorum_size,
            ),
            quorum_retries: QuorumRetries::new(write_quorum_size),
        }
    }
}

// `Caesar` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    MPropose {
        dot: Dot,
        cmd: Command,
        clock: Clock,
        quorum: HashSet<ProcessId>,
    },
    MProposeAck {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
    },
    MCommit {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
    },
    MRetry {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
    },
    MRetryAck {
        dot: Dot,
        deps: HashSet<Dot>,
    },
    MCommitDot {
        dot: Dot,
    },
    MGarbageCollection {
        committed: VClock<ProcessId>,
    },
    MStable {
        stable: Vec<(ProcessId, u64, u64)>,
    },
}

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{
            worker_dot_index_shift, worker_index_no_shift, GC_WORKER_INDEX,
        };
        // TODO: I think that we need that the dot info is shared across
        // workers, and in that case here we can do a random, and not a shift
        // based on the dot
        match self {
            // Protocol messages
            Self::MPropose { dot, .. } => worker_dot_index_shift(&dot),
            Self::MProposeAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MRetry { dot, .. } => worker_dot_index_shift(&dot),
            Self::MRetryAck { dot, .. } => worker_dot_index_shift(&dot),
            // GC messages
            Self::MCommitDot { .. } => worker_index_no_shift(GC_WORKER_INDEX),
            Self::MGarbageCollection { .. } => {
                worker_index_no_shift(GC_WORKER_INDEX)
            }
            Self::MStable { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl MessageIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{worker_index_no_shift, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    START,
    PAYLOAD,
    PROPOSE,
    REJECT,
    ACCEPT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, KeyGen, Workload};
    use fantoch::planet::{Planet, Region};
    use fantoch::sim::Simulation;
    use fantoch::time::SimTime;
    use fantoch::util;

    #[ignore]
    #[test]
    fn sequential_caesar_test() {
        caesar_flow::<SequentialKeyClocks>();
    }

    #[ignore]
    #[test]
    fn locked_caesar_test() {
        caesar_flow::<LockedKeyClocks>();
    }

    fn caesar_flow<KD: KeyClocks>() {
        // create simulation
        let mut simulation = Simulation::new();

        // processes ids
        let process_id_1 = 1;
        let process_id_2 = 2;
        let process_id_3 = 3;

        // regions
        let europe_west2 = Region::new("europe-west2");
        let europe_west3 = Region::new("europe-west2");
        let us_west1 = Region::new("europe-west2");

        // there's a single shard
        let shard_id = 0;

        // processes
        let processes = vec![
            (process_id_1, shard_id, europe_west2.clone()),
            (process_id_2, shard_id, europe_west3.clone()),
            (process_id_3, shard_id, us_west1.clone()),
        ];

        // planet
        let planet = Planet::new();

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // executors
        let executor_1 = GraphExecutor::new(process_id_1, shard_id, config);
        let executor_2 = GraphExecutor::new(process_id_2, shard_id, config);
        let executor_3 = GraphExecutor::new(process_id_3, shard_id, config);

        // caesar
        let (mut caesar_1, _) =
            Caesar::<KD>::new(process_id_1, shard_id, config);
        let (mut caesar_2, _) =
            Caesar::<KD>::new(process_id_2, shard_id, config);
        let (mut caesar_3, _) =
            Caesar::<KD>::new(process_id_3, shard_id, config);

        // discover processes in all caesar
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        caesar_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        caesar_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        caesar_3.discover(sorted);

        // register processes
        simulation.register_process(caesar_1, executor_1);
        simulation.register_process(caesar_2, executor_2);
        simulation.register_process(caesar_3, executor_3);

        // client workload
        let shard_count = 1;
        let key_gen = KeyGen::ConflictRate { conflict_rate: 100 };
        let keys_per_command = 1;
        let commands_per_client = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // create client 1 that is connected to caesar 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let status_frequency = None;
        let mut client_1 = Client::new(client_id, workload, status_frequency);

        // discover processes in client 1
        let closest =
            util::closest_process_per_shard(&client_region, &planet, processes);
        client_1.connect(closest);

        // start client
        let (target_shard, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is caesar 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in caesar 1
        let (process, _, pending, time) = simulation.get_process(target);
        pending.wait_for(&cmd);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mpropose = actions.pop().unwrap();

        // check that the mpropose is being sent to *all* processes
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mpropose.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle mproposes
        let mut mproposeacks =
            simulation.forward_to_processes((process_id_1, mpropose));

        // check that there are 3 mproposeacks
        assert_eq!(mproposeacks.len(), 3);

        // handle the first mproposeack
        let mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mproposeack
        let mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the third mproposeack
        let mut mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
        );
        // there's a commit now
        assert_eq!(mcommits.len(), 1);

        // check that the mcommit is sent to everyone
        let mcommit = mcommits.pop().expect("there should be an mcommit");
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mcommit.clone(), (_, Action::ToSend {target, ..}) if check_target(&target))
        );

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mcommit);

        // check the MCommitDot
        let check_msg = |msg: &Message| matches!(msg, Message::MCommitDot {..});
        assert!(to_sends.into_iter().all(|(_, action)| {
            matches!(action, Action::ToForward { msg } if check_msg(&msg))
        }));

        // process 1 should have something to the executor
        let (process, executor, pending, time) =
            simulation.get_process(process_id_1);
        let to_executor: Vec<_> = process.to_executors_iter().collect();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command partial
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| {
                executor.handle(info, time);
                executor.to_clients_iter().collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ready.len(), 1);

        // get that command
        let executor_result =
            ready.pop().expect("there should an executor result");
        let cmd_result = pending
            .add_executor_result(executor_result)
            .expect("there should be a command result");

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result)
            .expect("there should a new submit");

        let (process, _, _, time) = simulation.get_process(target);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mpropose = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MPropose {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(
            matches!(mpropose, Action::ToSend {msg, ..} if check_msg(&msg))
        );
    }
}
