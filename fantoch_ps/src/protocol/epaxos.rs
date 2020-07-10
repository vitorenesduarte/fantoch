use crate::executor::GraphExecutor;
use crate::protocol::common::graph::{
    KeyClocks, LockedKeyClocks, QuorumClocks, SequentialKeyClocks,
};
use crate::protocol::common::synod::{Synod, SynodMessage};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId};
use fantoch::protocol::{
    Action, BaseProcess, CommandsInfo, Info, MessageIndex, PeriodicEventIndex,
    Protocol, ProtocolMetrics,
};
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{log, singleton};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::mem;
use std::time::Duration;
use threshold::VClock;
use tracing::instrument;

pub type EPaxosSequential = EPaxos<SequentialKeyClocks>;
pub type EPaxosLocked = EPaxos<LockedKeyClocks>;

type ExecutionInfo = <GraphExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone)]
pub struct EPaxos<KC> {
    bp: BaseProcess,
    keys_clocks: KC,
    cmds: CommandsInfo<EPaxosInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl<KC: KeyClocks> Protocol for EPaxos<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = GraphExecutor;

    /// Creates a new `Atlas` process.
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) =
            config.epaxos_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let keys_clocks = KC::new(config.n());
        let f = Self::allowed_faults(config.n());
        let cmds =
            CommandsInfo::new(process_id, config.n(), f, fast_quorum_size);
        let to_executor = Vec::new();

        // create `EPaxos`
        let protocol = Self {
            bp,
            keys_clocks,
            cmds,
            to_executor,
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

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    fn discover(&mut self, processes: Vec<ProcessId>) -> bool {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        match msg {
            Message::MCollect {
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock, time),
            Message::MCollectAck { dot, clock } => {
                self.handle_mcollectack(from, dot, clock, time)
            }
            Message::MCommit { dot, value } => {
                self.handle_mcommit(from, dot, value, time)
            }
            Message::MConsensus { dot, ballot, value } => {
                self.handle_mconsensus(from, dot, ballot, value, time)
            }
            Message::MConsensusAck { dot, ballot } => {
                self.handle_mconsensusack(from, dot, ballot, time)
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
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
        }
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        mem::take(&mut self.to_executor)
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

impl<KC: KeyClocks> EPaxos<KC> {
    /// EPaxos always tolerates a minority of faults.
    pub fn allowed_faults(n: usize) -> usize {
        n / 2
    }

    /// Handles a submit operation by a client.
    #[instrument(skip(self, dot, cmd))]
    fn handle_submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
    ) -> Vec<Action<Self>> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // wrap command
        let cmd = Some(cmd);

        // compute its clock
        // - similarly to Atlas, here we don't save the command in
        //   `keys_clocks`; if we did, it would be declared as a dependency of
        //   itself when this message is handled by its own coordinator, which
        //   prevents fast paths with f > 1; in fact we do, but since the
        //   coordinator does not recompute this value in the MCollect handler,
        //   it's effectively the same
        let clock = self.keys_clocks.add(dot, &cmd, None);

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        vec![Action::ToSend {
            target,
            msg: mcollect,
        }]
    }

    #[instrument(skip(self, from, dot, cmd, quorum, remote_clock, time))]
    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Option<Command>,
        quorum: HashSet<ProcessId>,
        remote_clock: VClock<ProcessId>,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCollect({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from,
            time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return vec![];
        }

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;

        let clock = if message_from_self {
            // if it is, do not recompute clock
            remote_clock
        } else {
            // otherwise, compute clock with the remote clock as past
            self.keys_clocks.add(dot, &cmd, Some(remote_clock))
        };

        // update command info
        info.status = Status::COLLECT;
        info.quorum = quorum;
        // create and set consensus value
        let value = ConsensusValue::with(cmd, clock.clone());
        assert!(info.synod.set_if_not_accepted(|| value));

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck { dot, clock };
        let target = singleton![from];

        // return `ToSend`
        vec![Action::ToSend {
            target,
            msg: mcollectack,
        }]
    }

    #[instrument(skip(self, from, dot, clock, time))]
    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: VClock<ProcessId>,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCollectAck({:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            from,
            time.millis()
        );

        // ignore ack from self (see `EPaxosInfo::new` for the reason why)
        if from == self.bp.process_id {
            return vec![];
        }

        // get cmd info
        let info = self.cmds.get(dot);

        // do nothing if we're no longer COLLECT
        if info.status != Status::COLLECT {
            return vec![];
        }

        // update quorum clocks
        info.quorum_clocks.add(from, clock);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // compute the union while checking whether all clocks reported are
            // equal
            let (final_clock, all_equal) = info.quorum_clocks.union();

            // create consensus value
            // TODO can the following be more performant or at least more
            // ergonomic?
            let cmd = info.synod.value().cmd.clone();
            let value = ConsensusValue::with(cmd, final_clock);

            // fast path condition:
            // - all reported clocks if `max_clock` was reported by at least f
            //   processes
            if all_equal {
                self.bp.fast_path();
                // fast path: create `MCommit`
                // TODO create a slim-MCommit that only sends the payload to the
                // non-fast-quorum members, or send the payload
                // to all in a slim-MConsensus
                let mcommit = Message::MCommit { dot, value };
                let target = self.bp.all();

                // return `ToSend`
                vec![Action::ToSend {
                    target,
                    msg: mcommit,
                }]
            } else {
                self.bp.slow_path();
                // slow path: create `MConsensus`
                let ballot = info.synod.skip_prepare();
                let mconsensus = Message::MConsensus { dot, ballot, value };
                let target = self.bp.write_quorum();
                // return `ToSend`
                vec![Action::ToSend {
                    target,
                    msg: mconsensus,
                }]
            }
        } else {
            vec![]
        }
    }

    #[instrument(skip(self, from, dot, value, time))]
    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        value: ConsensusValue,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommit({:?}, {:?}) | time={}",
            self.id(),
            dot,
            value.clock,
            time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return vec![];
        }

        // update command info:
        info.status = Status::COMMIT;

        // handle commit in synod
        let msg = SynodMessage::MChosen(value.clone());
        assert!(info.synod.handle(from, msg).is_none());

        // create execution info if not a noop
        if let Some(cmd) = value.cmd {
            // create execution info
            let execution_info = ExecutionInfo::new(dot, cmd, value.clock);
            self.to_executor.push(execution_info);
        }

        if self.gc_running() {
            // notify self with the committed dot
            vec![Action::ToForward {
                msg: Message::MCommitDot { dot },
            }]
        } else {
            // if we're not running gc, remove the dot info now
            self.cmds.gc_single(dot);
            vec![]
        }
    }

    #[instrument(skip(self, from, dot, ballot, value, time))]
    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MConsensus({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            ballot,
            value.clock,
            time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing, an ack or an mcommit
        let msg = match info
            .synod
            .handle(from, SynodMessage::MAccept(ballot, value))
        {
            Some(SynodMessage::MAccepted(ballot)) => {
                // the accept message was accepted: create `MConsensusAck`
                Message::MConsensusAck { dot, ballot }
            }
            Some(SynodMessage::MChosen(value)) => {
                // the value has already been chosen: create `MCommit`
                Message::MCommit { dot, value }
            }
            None => {
                // ballot too low to be accepted
                return vec![];
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensus handler"
            ),
        };

        // create target
        let target = singleton![from];

        // return `ToSend`
        vec![Action::ToSend { target, msg }]
    }

    #[instrument(skip(self, from, dot, ballot, time))]
    fn handle_mconsensusack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MConsensusAck({:?}, {}) | time={}",
            self.id(),
            dot,
            ballot,
            time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing or an mcommit
        match info.synod.handle(from, SynodMessage::MAccepted(ballot)) {
            Some(SynodMessage::MChosen(value)) => {
                // enough accepts were gathered and the value has been chosen: create `MCommit` and target
                let target = self.bp.all();
                let mcommit = Message::MCommit { dot, value };

                // return `ToSend`
                vec![Action::ToSend {
                    target,
                    msg: mcommit,
                }]
            }
            None => {
                // not enough accepts yet
                vec![]
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensusAck handler"
            ),
        }
    }

    #[instrument(skip(self, from, dot, time))]
    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommitDot({:?}) | time={}",
            self.id(),
            dot,
            time.millis()
        );
        assert_eq!(from, self.bp.process_id);
        self.cmds.commit(dot);
        vec![]
    }

    #[instrument(skip(self, from, committed, time))]
    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            time.millis()
        );
        self.cmds.committed_by(from, committed);
        // compute newly stable dots
        let stable = self.cmds.stable();
        // create `ToForward` to self
        if stable.is_empty() {
            vec![]
        } else {
            vec![Action::ToForward {
                msg: Message::MStable { stable },
            }]
        }
    }

    #[instrument(skip(self, from, stable, time))]
    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MStable({:?}) from {} | time={}",
            self.id(),
            stable,
            from,
            time.millis()
        );
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
        vec![]
    }

    #[instrument(skip(self, time))]
    fn handle_event_garbage_collection(
        &mut self,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            time.millis()
        );

        // retrieve the committed clock
        let committed = self.cmds.committed();

        // create `ToSend`
        vec![Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        }]
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }
}

// consensus value is a pair where the first component is the command (noop if
// `None`) and the second component its dependencies represented as a vector
// clock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsensusValue {
    cmd: Option<Command>,
    clock: VClock<ProcessId>,
}

impl ConsensusValue {
    fn new(n: usize) -> Self {
        let cmd = None;
        let clock = VClock::with(util::process_ids(n));
        Self { cmd, clock }
    }

    fn with(cmd: Option<Command>, clock: VClock<ProcessId>) -> Self {
        Self { cmd, clock }
    }
}

fn proposal_gen(_values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
    todo!("recovery not implemented yet")
}

// `EPaxosInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone)]
struct EPaxosInfo {
    status: Status,
    quorum: HashSet<ProcessId>,
    synod: Synod<ConsensusValue>,
    // `quorum_clocks` is used by the coordinator to compute the threshold
    // clock when deciding whether to take the fast path
    quorum_clocks: QuorumClocks,
}

impl Info for EPaxosInfo {
    fn new(
        process_id: ProcessId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        // create bottom consensus value
        let initial_value = ConsensusValue::new(n);

        // although the fast quorum size is `fast_quorum_size`, we're going to
        // initialize `QuorumClocks` with `fast_quorum_size - 1` since
        // the clock reported by the coordinator shouldn't be considered
        // in the fast path condition, and this clock is not necessary for
        // correctness; for this to work, `MCollectAck`'s from self should be
        // ignored, or not even created.
        Self {
            status: Status::START,
            quorum: HashSet::new(),
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            quorum_clocks: QuorumClocks::new(fast_quorum_size - 1),
        }
    }
}

// `Atlas` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    MCollect {
        dot: Dot,
        cmd: Option<Command>, // it's never a noop though
        clock: VClock<ProcessId>,
        quorum: HashSet<ProcessId>,
    },
    MCollectAck {
        dot: Dot,
        clock: VClock<ProcessId>,
    },
    MCommit {
        dot: Dot,
        value: ConsensusValue,
    },
    MConsensus {
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
    },
    MConsensusAck {
        dot: Dot,
        ballot: u64,
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
        match self {
            // Protocol messages
            Self::MCollect { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCollectAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensus { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensusAck { dot, .. } => worker_dot_index_shift(&dot),
            // GC messages
            Self::MCommitDot { .. } => worker_index_no_shift(GC_WORKER_INDEX),
            Self::MGarbageCollection { .. } => {
                worker_index_no_shift(GC_WORKER_INDEX)
            }
            Self::MStable { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl PeriodicEventIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{worker_index_no_shift, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(Debug, PartialEq, Clone)]
enum Status {
    START,
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, KeyGen, Workload};
    use fantoch::planet::{Planet, Region};
    use fantoch::sim::Simulation;
    use fantoch::time::SimTime;

    #[test]
    fn sequential_epaxos_test() {
        epaxos_flow::<SequentialKeyClocks>();
    }

    #[test]
    fn locked_epaxos_test() {
        epaxos_flow::<LockedKeyClocks>();
    }

    fn epaxos_flow<KC: KeyClocks>() {
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

        // processes
        let processes = vec![
            (process_id_1, europe_west2.clone()),
            (process_id_2, europe_west3.clone()),
            (process_id_3, us_west1.clone()),
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
        let executor_1 = GraphExecutor::new(process_id_1, config, 0);
        let executor_2 = GraphExecutor::new(process_id_2, config, 0);
        let executor_3 = GraphExecutor::new(process_id_3, config, 0);

        // epaxos
        let (mut epaxos_1, _) = EPaxos::<KC>::new(process_id_1, config);
        let (mut epaxos_2, _) = EPaxos::<KC>::new(process_id_2, config);
        let (mut epaxos_3, _) = EPaxos::<KC>::new(process_id_3, config);

        // discover processes in all epaxos
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        epaxos_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        epaxos_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        epaxos_3.discover(sorted);

        // register processes
        simulation.register_process(epaxos_1, executor_1);
        simulation.register_process(epaxos_2, executor_2);
        simulation.register_process(epaxos_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let key_gen = KeyGen::ConflictRate { conflict_rate };
        let keys_per_command = 1;
        let total_commands = 10;
        let payload_size = 100;
        let workload = Workload::new(key_gen, keys_per_command, total_commands, payload_size);

        // create client 1 that is connected to epaxos 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, workload);

        // discover processes in client 1
        let sorted = util::sort_processes_by_distance(
            &client_region,
            &planet,
            processes,
        );
        assert!(client_1.discover(sorted));

        // start client
        let (target, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");

        // check that `target` is epaxos 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in epaxos 1
        let (process, executor, time) = simulation.get_process(target);
        executor.wait_for(&cmd);
        let mut actions = process.submit(None, cmd, time);
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        // check that the mcollect is being sent to 2 processes
        let check_target = |target: &HashSet<ProcessId>| {
            target.len() == 2 && target.contains(&1) && target.contains(&2)
        };
        assert!(
            matches!(mcollect.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle mcollects
        let mut mcollectacks =
            simulation.forward_to_processes((process_id_1, mcollect));

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the *only* mcollectack
        // - there's a single mcollectack single the initial coordinator does
        //   not reply to itself
        let mut mcommits = simulation.forward_to_processes(
            mcollectacks.pop().expect("there should be an mcollect ack"),
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
        let (process, executor, _) = simulation.get_process(process_id_1);
        let to_executor = process.to_executor();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command ready
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| executor.handle(info))
            .map(|result| result.unwrap_ready())
            .collect();
        assert_eq!(ready.len(), 1);

        // get that command
        let cmd_result = ready.pop().expect("there should a command ready");

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result)
            .expect("there should a new submit");

        let (process, _, time) = simulation.get_process(target);
        let mut actions = process.submit(None, cmd, time);
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MCollect {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(
            matches!(mcollect, Action::ToSend {msg, ..} if check_msg(&msg))
        );
    }
}
