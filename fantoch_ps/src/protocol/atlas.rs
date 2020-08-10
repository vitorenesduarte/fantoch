use crate::executor::GraphExecutor;
use crate::protocol::common::graph::{
    KeyClocks, LockedKeyClocks, QuorumClocks, SequentialKeyClocks,
};
use crate::protocol::common::synod::{Synod, SynodMessage};
use crate::protocol::partial::{self, ShardsCommits};
use crate::util;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{
    Action, BaseProcess, CommandsInfo, Info, MessageIndex, PeriodicEventIndex,
    Protocol, ProtocolMetrics,
};
use fantoch::time::SysTime;
use fantoch::{log, singleton};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use threshold::VClock;
use tracing::instrument;

pub type AtlasSequential = Atlas<SequentialKeyClocks>;
pub type AtlasLocked = Atlas<LockedKeyClocks>;

type ExecutionInfo = <GraphExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Atlas<KC: KeyClocks> {
    bp: BaseProcess,
    keys_clocks: KC,
    cmds: CommandsInfo<AtlasInfo>,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
    // set of processes in my shard
    shard_processes: HashSet<ProcessId>,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, ConsensusValue)>,
}

impl<KC: KeyClocks> Protocol for Atlas<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = GraphExecutor;

    /// Creates a new `Atlas` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) = config.atlas_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let keys_clocks = KC::new(shard_id, config.n());
        let cmds = CommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            config.f(),
            fast_quorum_size,
        );
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let shard_processes = util::process_ids(shard_id, config.n()).collect();
        let buffered_commits = HashMap::new();

        // create `Atlas`
        let protocol = Self {
            bp,
            keys_clocks,
            cmds,
            to_processes,
            to_executors,
            shard_processes,
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
    ) -> (bool, HashSet<ProcessId>) {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, dot: Option<Dot>, cmd: Command, _time: &dyn SysTime) {
        self.handle_submit(dot, cmd, true)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) {
        match msg {
            // Protocol messages
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
            // Partial replication
            Message::MForwardSubmit { dot, cmd } => {
                self.handle_submit(Some(dot), cmd, false)
            }
            Message::MShardCommit { dot, clock } => {
                self.handle_mshard_commit(from, from_shard_id, dot, clock, time)
            }
            Message::MShardAggregatedCommit { dot, clock } => {
                self.handle_mshard_aggregated_commit(dot, clock, time)
            }
            Message::MCommitExecutorInfo { dot, cmd, clock } => {
                self.handle_mcommit_executor_info(dot, cmd, clock, time)
            }
            // GC messages
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

impl<KC: KeyClocks> Atlas<KC> {
    /// Handles a submit operation by a client.
    #[instrument(skip(self, dot, cmd))]
    fn handle_submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        target_shard: bool,
    ) {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // create submit actions
        let create_mforward_submit =
            |dot, cmd| Message::MForwardSubmit { dot, cmd };
        partial::submit_actions(
            &self.bp,
            dot,
            &cmd,
            target_shard,
            create_mforward_submit,
            &mut self.to_processes,
        );

        // compute its clock
        // - here we don't save the command in `keys_clocks`; if we did, it
        //   would be declared as a dependency of itself when this message is
        //   handled by its own coordinator, which prevents fast paths with f >
        //   1; in fact we do, but since the coordinator does not recompute this
        //   value in the MCollect handler, it's effectively the same
        let clock = self.keys_clocks.add_cmd(dot, &cmd, None);

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.all();

        // add `Mcollect` send as action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollect,
        });
    }

    #[instrument(skip(self, from, dot, cmd, quorum, remote_clock, time))]
    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        remote_clock: VClock<ProcessId>,
        time: &dyn SysTime,
    ) {
        log!(
            "p{}: MCollect({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from,
            time.micros()
        );

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
            // - if we received the `MCommit` before the `MCollect`, handle the
            //   `MCommit` now

            info.status = Status::PAYLOAD;
            info.cmd = Some(cmd);

            // check if there's a buffered commit notification; if yes, handle
            // the commit again (since now we have the payload)
            if let Some((from, value)) = self.buffered_commits.remove(&dot) {
                self.handle_mcommit(from, dot, value, time);
            }
            return;
        }

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;

        let clock = if message_from_self {
            // if it is, do not recompute clock
            remote_clock
        } else {
            // otherwise, compute clock with the remote clock as past
            self.keys_clocks.add_cmd(dot, &cmd, Some(remote_clock))
        };

        // update command info
        info.status = Status::COLLECT;
        info.quorum = quorum;
        info.cmd = Some(cmd);
        // create and set consensus value
        let value = ConsensusValue::with(clock.clone());
        assert!(info.synod.set_if_not_accepted(|| value));

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck { dot, clock };
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollectack,
        });
    }

    #[instrument(skip(self, from, dot, clock, _time))]
    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MCollectAck({:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            from,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status != Status::COLLECT {
            // do nothing if we're no longer COLLECT
            return;
        }

        // update quorum clocks
        info.quorum_clocks.add(from, clock);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // compute the threshold union while checking whether it's equal to
            // their union
            let (final_clock, equal_to_union) =
                info.quorum_clocks.threshold_union(self.bp.config.f());

            // create consensus value
            let value = ConsensusValue::with(final_clock);

            // fast path condition:
            // - each dependency was reported by at least f processes
            if equal_to_union {
                self.bp.fast_path();
                // fast path: create `MCommit`
                let shard_count = info.cmd.as_ref().unwrap().shard_count();
                Self::mcommit_actions(
                    &self.bp,
                    info,
                    shard_count,
                    dot,
                    value,
                    &mut self.to_processes,
                )
            } else {
                self.bp.slow_path();

                // slow path: create `MConsensus`
                let ballot = info.synod.skip_prepare();
                let mconsensus = Message::MConsensus { dot, ballot, value };
                let target = self.bp.write_quorum();
                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mconsensus,
                });
            }
        }
    }

    #[instrument(skip(self, from, dot, value, _time))]
    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        value: ConsensusValue,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MCommit({:?}, {:?}) | time={}",
            self.id(),
            dot,
            value.clock,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::START {
            // TODO we missed the `MCollect` message and should try to recover
            // the payload:
            // - save this notification just in case we've received the
            //   `MCollect` and `MCommit` in opposite orders (due to
            //   multiplexing)
            self.buffered_commits.insert(dot, (from, value));
            return;
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return;
        }

        // check it's not a noop
        assert_eq!(
            value.is_noop, false,
            "handling noop's is not implemented yet"
        );

        // get command
        let cmd = info
            .cmd
            .as_ref()
            .expect("there should be a command payload");

        // forward commit executor info to other processes in my region that *do
        // not* replicate this command if:
        // - I'm in the target shard (this makes sure that only one of the
        //   processes that *do* replicate the command will send this
        //   information)
        // note that we may even have to do this for single-shard commands.
        // consider for example that we have two shards, A and B, where A
        // replicates key x and B replicates key y. consider also two commands,
        // C1 = write(x) and C2 = write(x, y). it can happen that dep[C1] = { }
        // and dep[C2] = {C1}. note that C1 will only be committed in shard
        // A, while C2 will be committed in both shards. for this reason, only
        // one of the shards (shard A) has all the dependencies of
        // command C2 (i.e. C1). thus, the commit info of C1 also needs to be
        // sent to shard B, even though C1 is a single-shard command.
        //
        // note that we're being pessimistic here since we're assuming that all
        // committed commands may be needed in the other shards. I feel like
        // this is a reasonable approach since the bottleneck in this protocol
        // will always be in the non-parallel executor and if we were to request
        // this information on demand (AKA partition chasing) it would be much
        // more complex and not at all more efficient.
        if self.shard_processes.contains(&dot.source()) {
            // ignore processes in region that already have this information
            let target = self.bp.all_in_region_but(cmd);
            self.to_processes.push(Action::ToSend {
                target,
                msg: Message::MCommitExecutorInfo {
                    dot,
                    cmd: cmd.clone(),
                    clock: value.clock.clone(),
                },
            })
        }

        // create execution info
        let execution_info =
            ExecutionInfo::add(dot, cmd.clone(), value.clock.clone());
        self.to_executors.push(execution_info);

        // update command info:
        info.status = Status::COMMIT;

        // handle commit in synod
        let msg = SynodMessage::MChosen(value);
        assert!(info.synod.handle(from, msg).is_none());

        // check if this dot is targetted to my shard
        // TODO: fix this once we implement recovery for partial replication
        let my_shard = util::process_ids(self.bp.shard_id, self.bp.config.n())
            .any(|peer_id| peer_id == dot.source());

        if self.gc_running() && my_shard {
            // if running gc and this dot belongs to my shard, then notify self
            // (i.e. the worker responsible for GC) with the committed dot
            self.to_processes.push(Action::ToForward {
                msg: Message::MCommitDot { dot },
            });
        } else {
            // not running gc, so remove the dot info now
            self.cmds.gc_single(dot);
        }
    }

    #[instrument(skip(self, from, dot, ballot, value, _time))]
    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MConsensus({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            ballot,
            value.clock,
            _time.micros()
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
                Message::MCommit { dot, value}
            }
            None => {
                // ballot too low to be accepted: nothing to do
                return;
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensus handler"
            ),
        };

        // create target
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend { target, msg });
    }

    #[instrument(skip(self, from, dot, ballot, _time))]
    fn handle_mconsensusack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MConsensusAck({:?}, {}) | time={}",
            self.id(),
            dot,
            ballot,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing or an mcommit
        match info.synod.handle(from, SynodMessage::MAccepted(ballot)) {
            Some(SynodMessage::MChosen(value)) => {
                // enough accepts were gathered and the value has been chosen; create `MCommit`
                let shard_count = info.cmd.as_ref().unwrap().shard_count();
                Self::mcommit_actions(&self.bp, info, shard_count, dot, value, &mut self.to_processes)
            }
            None => {
                // not enough accepts yet: nothing to do
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensusAck handler"
            ),
        }
    }

    #[instrument(skip(self, from, _from_shard_id, dot, clock, _time))]
    fn handle_mshard_commit(
        &mut self,
        from: ProcessId,
        _from_shard_id: ShardId,
        dot: Dot,
        clock: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MShardCommit({:?}, {:?}) from shard {} | time={}",
            self.id(),
            dot,
            clock,
            _from_shard_id,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        let shard_count = info.cmd.as_ref().unwrap().shard_count();
        let add_shards_commits_info =
            |max_clock: &mut VClock<ProcessId>, clock| max_clock.join(&clock);
        let create_mshard_aggregated_commit =
            |dot, max_clock: &VClock<ProcessId>| {
                Message::MShardAggregatedCommit {
                    dot,
                    clock: max_clock.clone(),
                }
            };

        partial::handle_mshard_commit(
            &self.bp,
            &mut info.shards_commits,
            shard_count,
            from,
            dot,
            clock,
            add_shards_commits_info,
            create_mshard_aggregated_commit,
            &mut self.to_processes,
        )
    }

    #[instrument(skip(self, dot, clock, _time))]
    fn handle_mshard_aggregated_commit(
        &mut self,
        dot: Dot,
        clock: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MShardAggregatedCommit({:?}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // nothing else to extract
        let extract_mcommit_extra_data = |_| ();
        let create_mcommit = |dot, clock, ()| {
            let value = ConsensusValue::with(clock);
            Message::MCommit { dot, value }
        };

        partial::handle_mshard_aggregated_commit(
            &self.bp,
            &mut info.shards_commits,
            dot,
            clock,
            extract_mcommit_extra_data,
            create_mcommit,
            &mut self.to_processes,
        )
    }

    #[instrument(skip(self, dot, cmd, clock, _time))]
    fn handle_mcommit_executor_info(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MCommitExecutorInfo({:?}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            _time.micros()
        );

        // create execution info
        let execution_info = ExecutionInfo::add(dot, cmd, clock);
        self.to_executors.push(execution_info);
    }

    #[instrument(skip(self, from, dot, _time))]
    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MCommitDot({:?}) | time={}",
            self.id(),
            dot,
            _time.micros()
        );
        assert_eq!(from, self.bp.process_id);
        self.cmds.commit(dot);
    }

    #[instrument(skip(self, from, committed, _time))]
    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            _time.micros()
        );
        self.cmds.committed_by(from, committed);
        // compute newly stable dots
        let stable = self.cmds.stable();
        // create `ToForward` to self
        if !stable.is_empty() {
            self.to_processes.push(Action::ToForward {
                msg: Message::MStable { stable },
            });
        }
    }

    #[instrument(skip(self, from, stable, _time))]
    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
        _time: &dyn SysTime,
    ) {
        log!(
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

    #[instrument(skip(self, _time))]
    fn handle_event_garbage_collection(&mut self, _time: &dyn SysTime) {
        log!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.micros()
        );

        // retrieve the committed clock
        let committed = self.cmds.committed();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    fn mcommit_actions(
        bp: &BaseProcess,
        info: &mut AtlasInfo,
        shard_count: usize,
        dot: Dot,
        value: ConsensusValue,
        to_processes: &mut Vec<Action<Self>>,
    ) {
        let create_mcommit = |dot, value, ()| Message::MCommit { dot, value };
        let create_mshard_commit =
            |dot, value: ConsensusValue| Message::MShardCommit {
                dot,
                clock: value.clock,
            };
        // nothing to update
        let update_shards_commit_info = |_: &mut VClock<ProcessId>, ()| {};

        partial::mcommit_actions(
            bp,
            &mut info.shards_commits,
            shard_count,
            dot,
            value,
            (),
            create_mcommit,
            create_mshard_commit,
            update_shards_commit_info,
            to_processes,
        )
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }
}

// consensus value is a pair where the first component is a flag indicating
// whether this is a noop and the second component is the commands dependencies
// represented as a vector clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusValue {
    is_noop: bool,
    clock: VClock<ProcessId>,
}

impl ConsensusValue {
    fn bottom(shard_id: ShardId, n: usize) -> Self {
        let is_noop = false;
        let clock = VClock::with(util::process_ids(shard_id, n));
        Self { is_noop, clock }
    }

    fn with(clock: VClock<ProcessId>) -> Self {
        let is_noop = false;
        Self { is_noop, clock }
    }
}

fn proposal_gen(_values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
    todo!("recovery not implemented yet")
}

// `AtlasInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone, PartialEq, Eq)]
struct AtlasInfo {
    status: Status,
    quorum: HashSet<ProcessId>,
    synod: Synod<ConsensusValue>,
    // `None` if not set yet
    cmd: Option<Command>,
    // `quorum_clocks` is used by the coordinator to compute the threshold
    // clock when deciding whether to take the fast path
    quorum_clocks: QuorumClocks,
    // `shard_commits` is only used when commands accessed more than one shard
    shards_commits: Option<ShardsCommits<VClock<ProcessId>>>,
}

impl Info for AtlasInfo {
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        // create bottom consensus value
        let initial_value = ConsensusValue::bottom(shard_id, n);
        Self {
            status: Status::START,
            quorum: HashSet::new(),
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            cmd: None,
            quorum_clocks: QuorumClocks::new(fast_quorum_size),
            shards_commits: None,
        }
    }
}

// `Atlas` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    // Protocol messages
    MCollect {
        dot: Dot,
        cmd: Command,
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
    // Partial replication messages
    MForwardSubmit {
        dot: Dot,
        cmd: Command,
    },
    MShardCommit {
        dot: Dot,
        clock: VClock<ProcessId>,
    },
    MShardAggregatedCommit {
        dot: Dot,
        clock: VClock<ProcessId>,
    },
    MCommitExecutorInfo {
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
    },
    // GC messages
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
            // Partial replication messages
            Self::MForwardSubmit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MShardCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MShardAggregatedCommit { dot, .. } => {
                worker_dot_index_shift(&dot)
            }
            Self::MCommitExecutorInfo { dot, .. } => {
                worker_dot_index_shift(&dot)
            }
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

impl PeriodicEventIndex for PeriodicEvent {
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
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, KeyGen, ShardGen, Workload};
    use fantoch::planet::{Planet, Region};
    use fantoch::sim::Simulation;
    use fantoch::time::SimTime;

    #[test]
    fn sequential_atlas_test() {
        atlas_flow::<SequentialKeyClocks>()
    }

    #[test]
    fn locked_atlas_test() {
        atlas_flow::<LockedKeyClocks>()
    }

    fn atlas_flow<KC: KeyClocks>() {
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

        // atlas
        let (mut atlas_1, _) = Atlas::<KC>::new(process_id_1, shard_id, config);
        let (mut atlas_2, _) = Atlas::<KC>::new(process_id_2, shard_id, config);
        let (mut atlas_3, _) = Atlas::<KC>::new(process_id_3, shard_id, config);

        // discover processes in all atlas
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        atlas_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        atlas_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        atlas_3.discover(sorted);

        // register processes
        simulation.register_process(atlas_1, executor_1);
        simulation.register_process(atlas_2, executor_2);
        simulation.register_process(atlas_3, executor_3);

        // client workload
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shard_count: 1 };
        let keys_per_shard = 1;
        let key_gen = KeyGen::ConflictRate { conflict_rate: 100 };
        let total_commands = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );

        // create client 1 that is connected to atlas 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, workload);

        // discover processes in client 1
        let closest =
            util::closest_process_per_shard(&client_region, &planet, processes);
        client_1.connect(closest);

        // start client
        let (target_shard, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is atlas 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in atlas 1
        let (process, _, pending, time) = simulation.get_process(target);
        pending.wait_for(&cmd);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        // check that the mcollect is being sent to *all* processes
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mcollect.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle mcollects
        let mut mcollectacks =
            simulation.forward_to_processes((process_id_1, mcollect));

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the first mcollectack
        let mcommits = simulation.forward_to_processes(
            mcollectacks.pop().expect("there should be an mcollect ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mcollectack
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
        let (process, executor, pending, _) =
            simulation.get_process(process_id_1);
        let to_executor: Vec<_> = process.to_executors_iter().collect();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command partial
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| {
                executor.handle(info);
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
        let mcollect = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MCollect {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(
            matches!(mcollect, Action::ToSend {msg, ..} if check_msg(&msg))
        );
    }
}
