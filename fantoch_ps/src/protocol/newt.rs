use crate::executor::TableExecutor;
use crate::protocol::common::synod::{Synod, SynodMessage};
use crate::protocol::common::table::{
    AtomicKeyClocks, FineLockedKeyClocks, KeyClocks, LockedKeyClocks,
    QuorumClocks, SequentialKeyClocks, Votes,
};
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
use std::mem;
use std::time::Duration;
use threshold::VClock;
use tracing::instrument;

pub type NewtSequential = Newt<SequentialKeyClocks>;
pub type NewtAtomic = Newt<AtomicKeyClocks>;
pub type NewtLocked = Newt<LockedKeyClocks>;
pub type NewtFineLocked = Newt<FineLockedKeyClocks>;

type ExecutionInfo = <TableExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Newt<KC> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: CommandsInfo<NewtInfo>,
    to_executor: Vec<ExecutionInfo>,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, u64, Votes)>,
    // With many many operations, it can happen that logical clocks are
    // higher that current time (if it starts at 0), and in that case,
    // the real time feature of newt doesn't work. Solution: track the highest
    // committed clock; when periodically bumping with real time, use this
    // value as the minimum value to bump to
    max_commit_clock: u64,
    skip_fast_ack: bool,
}

impl<KC: KeyClocks> Protocol for Newt<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = TableExecutor;

    /// Creates a new `Newt` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size, _) =
            config.newt_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_clocks = KC::new(process_id, shard_id);
        let cmds = CommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            config.f(),
            fast_quorum_size,
        );
        let to_executor = Vec::new();
        let buffered_commits = HashMap::new();
        let max_commit_clock = 0;
        // enable skip fast ack if configured like that and the fast quorum size
        // is 2
        let skip_fast_ack = config.skip_fast_ack() && fast_quorum_size == 2;

        // create `Newt`
        let protocol = Self {
            bp,
            key_clocks,
            cmds,
            to_executor,
            buffered_commits,
            max_commit_clock,
            skip_fast_ack,
        };

        // maybe create garbage collection periodic event
        let mut events = if let Some(interval) = config.gc_interval() {
            vec![(PeriodicEvent::GarbageCollection, interval)]
        } else {
            vec![]
        };

        // maybe create clock bump periodic event
        if let Some(interval) = config.newt_clock_bump_interval() {
            events.reserve_exact(1);
            events.push((PeriodicEvent::ClockBump, interval));
        }

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
    fn discover(&mut self, processes: Vec<(ProcessId, ShardId)>) -> bool {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        self.handle_submit(dot, cmd, true)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        match msg {
            // Protocol messages
            Message::MCollect {
                dot,
                cmd,
                quorum,
                clock,
                coordinator_votes,
            } => self.handle_mcollect(
                from,
                dot,
                cmd,
                quorum,
                clock,
                coordinator_votes,
                time,
            ),
            Message::MCollectAck {
                dot,
                clock,
                process_votes,
            } => self.handle_mcollectack(from, dot, clock, process_votes, time),
            Message::MCommit { dot, clock, votes } => {
                self.handle_mcommit(from, dot, clock, votes, time)
            }
            Message::MCommitClock { clock } => {
                self.handle_mcommit_clock(from, clock, time)
            }
            Message::MDetached { detached } => {
                self.handle_mdetached(detached, time)
            }
            Message::MConsensus { dot, ballot, clock } => {
                self.handle_mconsensus(from, dot, ballot, clock, time)
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
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
            PeriodicEvent::ClockBump => self.handle_event_clock_bump(time),
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

impl<KC: KeyClocks> Newt<KC> {
    /// Handles a submit operation by a client.
    #[instrument(skip(self, dot, cmd))]
    fn handle_submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        target_shard: bool,
    ) -> Vec<Action<Self>> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // create submit actions
        let mut actions = self.submit_actions(dot, &cmd, target_shard);
        log!(
            "p{}: submit extra actions for {:?}: {:?}",
            self.id(),
            dot,
            actions
        );

        // compute its clock:
        // - this may also consume votes since we're bumping the clocks here
        // - for that reason, we'll store these votes locally and not recompute
        //   them once we receive the `MCollect` from self
        let (clock, process_votes) = self.key_clocks.bump_and_vote(&cmd, 0);
        log!(
            "p{}: bump_and_vote: {:?} | clock: {} | votes: {:?}",
            self.id(),
            dot,
            clock,
            process_votes
        );

        // get shard count
        let shard_count = cmd.shard_count();

        // send votes if we can bypass the mcollectack, otherwise store them
        // - if the command acesses more than one shard, the optimization is
        //   disabled
        let coordinator_votes = if self.skip_fast_ack && shard_count == 1 {
            process_votes
        } else {
            // get cmd info
            let info = self.cmds.get(dot);
            info.votes = process_votes;
            Votes::new()
        };

        // create `MCollect` and target
        // TODO maybe just don't send to self with `self.bp.all_but_me()`
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            coordinator_votes,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.all();

        // add `MCollect` send as action and return all actions
        actions.push(Action::ToSend {
            target,
            msg: mcollect,
        });
        actions
    }

    #[instrument(skip(
        self,
        from,
        dot,
        cmd,
        quorum,
        remote_clock,
        votes,
        time
    ))]
    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        remote_clock: u64,
        mut votes: Votes,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCollect({:?}, {:?}, {:?}, {}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            quorum,
            remote_clock,
            votes,
            from,
            time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return vec![];
        }

        // check if part of fast quorum
        if !quorum.contains(&self.bp.process_id) {
            if self.bp.config.newt_clock_bump_interval().is_some() {
                // make sure there's a clock for each existing key:
                // - this ensures that all clocks will be bumped in the periodic
                //   clock bump event
                self.key_clocks.init_clocks(&cmd);
            }

            // if not, simply save the payload and set status to `PENDING`
            info.status = Status::PENDING;
            info.cmd = Some(cmd);

            // check if there's a buffered commit notification; if yes, handle
            // the commit again (since now we have the payload)
            if let Some((from, clock, votes)) =
                self.buffered_commits.remove(&dot)
            {
                return self.handle_mcommit(from, dot, clock, votes, time);
            } else {
                return vec![];
            }
        }

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;

        let (clock, process_votes) = if message_from_self {
            // if it is, do not recompute clock and votes
            (remote_clock, Votes::new())
        } else {
            // otherwise, compute clock considering the `remote_clock` as its
            // minimum value
            let (clock, process_votes) =
                self.key_clocks.bump_and_vote(&cmd, remote_clock);
            log!(
                "p{}: bump_and_vote: {:?} | clock: {} | votes: {:?}",
                self.bp.process_id,
                dot,
                clock,
                process_votes
            );
            // check that there's one vote per key
            debug_assert_eq!(
                process_votes.len(),
                cmd.key_count(self.bp.shard_id)
            );
            (clock, process_votes)
        };

        // get shard count
        let shard_count = cmd.shard_count();

        // update command info
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        // set consensus value
        assert!(info.synod.set_if_not_accepted(|| clock));

        if !message_from_self && self.skip_fast_ack && shard_count == 1 {
            votes.merge(process_votes);

            // if tiny quorums and f = 1, the fast quorum process can commit the
            // command right away; create `MCommit`
            let shard_count = info.cmd.as_ref().unwrap().shard_count();
            self.commit_actions(dot, clock, votes, shard_count)
        } else {
            // create `MCollectAck` and target
            let mcollectack = Message::MCollectAck {
                dot,
                clock,
                process_votes,
            };
            let target = singleton![from];

            // return `ToSend`
            vec![Action::ToSend {
                target,
                msg: mcollectack,
            }]
        }
    }

    #[instrument(skip(self, from, dot, clock, remote_votes, _time))]
    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        remote_votes: Votes,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCollectAck({:?}, {}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            remote_votes,
            from,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status != Status::COLLECT {
            // do nothing if we're no longer COLLECT
            return vec![];
        }

        // update votes with remote votes
        info.votes.merge(remote_votes);

        // update quorum clocks while computing max clock and its number of
        // occurrences
        let (max_clock, max_count) = info.quorum_clocks.add(from, clock);

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;

        // optimization: bump all keys clocks in `cmd` to be `max_clock`
        // - this prevents us from generating votes (either when clients submit
        //   new operations or when handling `MCollect` from other processes)
        //   that could potentially delay the execution of this command
        // - when skipping the mcollectack by fast quorum processes, the
        //   coordinator can't vote here; if it does, the votes generated here
        //   will never be sent in the MCommit message
        // - TODO: if we refactor votes to attached/detached business, then this
        //   is no longer a problem
        //
        // - TODO: it also seems that this (or the MCommit equivalent) must run
        //   with real time, otherwise there's a huge tail; but that don't make
        //   any sense
        if !message_from_self {
            let cmd = info.cmd.as_ref().unwrap();
            let detached = self.key_clocks.vote(cmd, max_clock);
            // update votes with detached
            info.votes.merge(detached);
        }

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // fast path condition:
            // - if `max_clock` was reported by at least f processes
            if max_count >= self.bp.config.f() {
                self.bp.fast_path();
                // reset local votes as we're going to receive them right away;
                // this also prevents a `info.votes.clone()`
                let votes = Self::reset_votes(&mut info.votes);

                // create `MCommit`
                let shard_count = info.cmd.as_ref().unwrap().shard_count();
                self.commit_actions(dot, max_clock, votes, shard_count)
            } else {
                self.bp.slow_path();
                // slow path: create `MConsensus`
                let ballot = info.synod.skip_prepare();
                let mconsensus = Message::MConsensus {
                    dot,
                    ballot,
                    clock: max_clock,
                };
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

    #[instrument(skip(self, from, dot, clock, _time))]
    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        mut votes: Votes,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommit({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            votes,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::START {
            // TODO we missed the `MCollect` message and should try to recover
            // the payload:
            // - save this notification just in case we've received the
            //   `MCollect` and `MCommit` in opposite orders (due to
            //   multiplexing)
            self.buffered_commits.insert(dot, (from, clock, votes));
            return vec![];
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return vec![];
        }

        // update command info:
        info.status = Status::COMMIT;

        // handle commit in synod
        let msg = SynodMessage::MChosen(clock);
        assert!(info.synod.handle(from, msg).is_none());

        // create execution info if not a noop
        let cmd = info.cmd.clone().expect("there should be a command payload");
        // create execution info
        let rifl = cmd.rifl();
        let execution_info =
            cmd.into_iter(self.bp.shard_id).map(|(key, op)| {
                // find votes on this key
                let key_votes = votes
                    .remove(&key)
                    .expect("there should be votes on all command keys");
                ExecutionInfo::votes(dot, clock, rifl, key, op, key_votes)
            });
        self.to_executor.extend(execution_info);

        // don't try to generate detached votes if configured with real time
        // (since it will be done in a periodic event)
        let mut actions = {
            if self.bp.config.newt_clock_bump_interval().is_some() {
                // in this case, only notify the clock bump worker of the commit
                // clock
                vec![Action::ToForward {
                    msg: Message::MCommitClock { clock },
                }]
            } else {
                // try to generate detached votes
                let cmd = info.cmd.as_ref().unwrap();
                let detached = self.key_clocks.vote(cmd, clock);
                if detached.is_empty() {
                    vec![]
                } else {
                    vec![Action::ToSend {
                        target: self.bp.all(),
                        msg: Message::MDetached { detached },
                    }]
                }
            }
        };

        if self.gc_running() {
            // running gc, so notify self with the committed dot
            actions.reserve_exact(1);
            actions.push(Action::ToForward {
                msg: Message::MCommitDot { dot },
            });
        } else {
            // not running gc, so remove the dot info now
            self.cmds.gc_single(dot);
        }
        actions
    }

    #[instrument(skip(self, from, clock, _time))]
    fn handle_mcommit_clock(
        &mut self,
        from: ProcessId,
        clock: u64,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommitClock({}) | time={}",
            self.id(),
            clock,
            _time.millis()
        );
        assert_eq!(from, self.bp.process_id);

        // simply update the highest commit clock
        self.max_commit_clock = std::cmp::max(self.max_commit_clock, clock);

        // nothing to send
        vec![]
    }

    #[instrument(skip(self, detached, _time))]
    fn handle_mdetached(
        &mut self,
        detached: Votes,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MDetached({:?}) | time={}",
            self.id(),
            detached,
            _time.millis()
        );

        // create execution info
        let execution_info = detached.into_iter().map(|(key, key_votes)| {
            ExecutionInfo::detached_votes(key, key_votes)
        });
        self.to_executor.extend(execution_info);

        // nothing to send
        vec![]
    }

    #[instrument(skip(self, from, dot, ballot, clock, _time))]
    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        clock: ConsensusValue,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MConsensus({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            ballot,
            clock,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing, an ack or an mcommit
        let msg = match info
            .synod
            .handle(from, SynodMessage::MAccept(ballot, clock))
        {
            Some(SynodMessage::MAccepted(ballot)) => {
                // the accept message was accepted: create `MConsensusAck`
                Message::MConsensusAck { dot, ballot }
            }
            Some(SynodMessage::MChosen(clock)) => {
                // the value has already been chosen: fetch votes and create `MCommit`
                // TODO: check if in recovery we will have enough votes to make the command stable
                let votes = info.votes.clone();
                Message::MCommit { dot, clock, votes }
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

    #[instrument(skip(self, from, dot, ballot, _time))]
    fn handle_mconsensusack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MConsensusAck({:?}, {}) | time={}",
            self.id(),
            dot,
            ballot,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing or an mcommit
        match info.synod.handle(from, SynodMessage::MAccepted(ballot)) {
            Some(SynodMessage::MChosen(clock)) => {
                // reset local votes as we're going to receive them right away;
                // this also prevents a `info.votes.clone()`
                // TODO: check if in recovery we will have enough votes to make the command stable
                let votes = Self::reset_votes(&mut info.votes);

                // enough accepts were gathered and the value has been chosen; create `MCommit`
                let shard_count = info.cmd.as_ref().unwrap().shard_count();
                self.commit_actions(dot, clock, votes, shard_count)
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

    #[instrument(skip(self, from, _from_shard_id, dot, clock, _time))]
    fn handle_mshard_commit(
        &mut self,
        from: ProcessId,
        _from_shard_id: ShardId,
        dot: Dot,
        clock: u64,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MShardCommit({:?}, {}) from shard {} | time={}",
            self.id(),
            dot,
            clock,
            _from_shard_id,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // make sure shards commit info is initialized:
        // - it may not be if we receive the `MCommitShard` from another shard
        //   before we were able to commit the command in our own shard
        let shards_commits =
            if let Some(shards_commits) = info.shards_commits.as_mut() {
                shards_commits
            } else {
                let process_id = self.bp.process_id;
                let shard_count = info.cmd.as_ref().unwrap().shard_count();
                info.shards_commits =
                    Some(ShardsCommits::new(process_id, shard_count));
                info.shards_commits.as_mut().unwrap()
            };

        // add new clock, checking if we have received all clocks
        let done = shards_commits.add(from, clock);
        if done {
            // create `MShardAggregatedCommit`
            let mshard_aggregated_commit = Message::MShardAggregatedCommit {
                dot,
                clock: shards_commits.max_clock,
            };
            let target = shards_commits.participants.clone();

            // return `ToSend`
            vec![Action::ToSend {
                target,
                msg: mshard_aggregated_commit,
            }]
        } else {
            Vec::new()
        }
    }

    #[instrument(skip(self, dot, clock, _time))]
    fn handle_mshard_aggregated_commit(
        &mut self,
        dot: Dot,
        clock: u64,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MShardAggregatedCommit({:?}, {}) | time={}",
            self.id(),
            dot,
            clock,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // take shards commit info
        let shards_commits = if let Some(shards_commits) =
            info.shards_commits.take()
        {
            shards_commits
        } else {
            panic!("no shards commit info when handling MShardAggregatedCommit about dot {:?}", dot)
        };

        // get votes
        let votes = shards_commits
            .votes
            .expect("votes in shard commit info should be set");

        // create `MCommit`
        let mcommit = Message::MCommit { dot, clock, votes };
        let target = self.bp.all();

        // return `ToSend`
        vec![Action::ToSend {
            target,
            msg: mcommit,
        }]
    }

    #[instrument(skip(self, from, dot, _time))]
    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommitDot({:?}) | time={}",
            self.id(),
            dot,
            _time.millis()
        );
        assert_eq!(from, self.bp.process_id);
        // TODO only call this if dot belongs to this shard
        self.cmds.commit(dot);
        vec![]
    }

    #[instrument(skip(self, from, committed, _time))]
    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            _time.millis()
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

    #[instrument(skip(self, from, stable, _time))]
    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MStable({:?}) from {} | time={}",
            self.id(),
            stable,
            from,
            _time.millis()
        );
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
        vec![]
    }

    #[instrument(skip(self, _time))]
    fn handle_event_garbage_collection(
        &mut self,
        _time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.millis()
        );

        // retrieve the committed clock
        let committed = self.cmds.committed();

        // create `ToSend`
        vec![Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        }]
    }

    #[instrument(skip(self, time))]
    fn handle_event_clock_bump(
        &mut self,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!("p{}: PeriodicEvent::ClockBump", self.id());

        // vote up to:
        // - highest committed clock or
        // - the current time,
        // whatever is the highest.
        // The fact that micros as used here is not an accident. With many
        // clients (like 1024 per site with 5 sites), millis do not provide
        // "enough precision".
        let min_clock = std::cmp::max(self.max_commit_clock, time.micros());

        // iterate all clocks and bump them to the current time:
        // - TODO: only bump the clocks of active keys (i.e. keys with an
        //   `MCollect` without the  corresponding `MCommit`)
        let detached = self.key_clocks.vote_all(min_clock);

        // create `ToSend`
        vec![Action::ToSend {
            target: self.bp.all(),
            msg: Message::MDetached { detached },
        }]
    }

    fn submit_actions(
        &self,
        dot: Dot,
        cmd: &Command,
        target_shard: bool,
    ) -> Vec<Action<Self>> {
        if target_shard {
            // action_count = (shard_count - 1) MForwardSubmit + 1 MCollect =
            // shard_count
            let action_count = cmd.shard_count();
            let mut actions = Vec::with_capacity(action_count);

            // create forward submit messages if:
            // - we're the target shard (i.e. the shard to which the client sent
            //   the command)
            // - command touches more than one shard
            for shard_id in cmd
                .shards()
                .filter(|shard_id| **shard_id != self.shard_id())
            {
                let mforward_submit = Message::MForwardSubmit {
                    dot,
                    cmd: cmd.clone(),
                };
                let target =
                    singleton![self.bp.closest_shard_process(shard_id)];
                actions.push(Action::ToSend {
                    target,
                    msg: mforward_submit,
                })
            }
            actions
        } else {
            // action_count = 1 MCollect
            let action_count = 1;
            Vec::with_capacity(action_count)
        }
    }

    fn commit_actions(
        &mut self,
        dot: Dot,
        clock: u64,
        votes: Votes,
        shard_count: usize,
    ) -> Vec<Action<Self>> {
        match shard_count {
            1 => {
                let mcommit = Message::MCommit { dot, clock, votes };
                let target = self.bp.all();
                vec![Action::ToSend {
                    target,
                    msg: mcommit,
                }]
            }
            _ => {
                // if the command accesses more than one shard, send an
                // MCommitShard to the process in the shard targetted by the
                // client; this process will then aggregate all the MCommitShard
                // and send an MCommitShardAggregated back once it receives an
                // MCommitShard from each shard; assuming that all
                // shards take the fast path, this approach should work well; if
                // there are slow paths, we probably want to disseminate each
                // shard commit clock to all participants so that detached votes
                // are generated ASAP; with n = 3 or f = 1, this is not a
                // problem since we'll always take the fast path
                // - TODO: revisit this approach once we implement recovery for
                //   partial replication
                // create shards commit info
                let info = self.cmds.get(dot);
                // initialize shards commit info if not yet initialized:
                // - it may already be initialized if we receive the
                //   `MCommitShard` from another shard before we were able to
                //   commit the command in our own shard
                if let Some(shards_commits) = info.shards_commits.as_mut() {
                    // if already initialized, simply save votes
                    shards_commits.set_votes(votes);
                } else {
                    // otherwise, initialize it (and also save votes)
                    let process_id = self.bp.process_id;
                    let shard_count = info.cmd.as_ref().unwrap().shard_count();
                    let mut shards_commits =
                        ShardsCommits::new(process_id, shard_count);
                    shards_commits.set_votes(votes);
                    info.shards_commits = Some(shards_commits);
                };

                // create `MShardCommit`
                let mshard_commit = Message::MShardCommit { dot, clock };
                // the aggregation with occurs at the process in targetted shard
                let target = singleton!(dot.source());
                vec![Action::ToSend {
                    target,
                    msg: mshard_commit,
                }]
            }
        }
    }

    // Replaces the value `local_votes` with empty votes, returning the previous
    // votes.
    fn reset_votes(local_votes: &mut Votes) -> Votes {
        mem::take(local_votes)
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }
}

// consensus value is simply the final clock for the command; this differs from
// `Atlas` where the command payload must also be part of consensus.
type ConsensusValue = u64;

fn proposal_gen(_values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
    todo!("recovery not implemented yet")
}

// `NewtInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone, PartialEq, Eq)]
struct NewtInfo {
    status: Status,
    quorum: HashSet<ProcessId>,
    synod: Synod<u64>,
    // `None` if not set yet (not like in `Atlas` where this `None` would be a
    // `noOp` - there are no `noOp`'s in `Newt`)
    cmd: Option<Command>,
    // `votes` is used by the coordinator to aggregate `ProcessVotes` from fast
    // quorum members
    votes: Votes,
    // `quorum_clocks` is used by the coordinator to compute the highest clock
    // reported by fast quorum members and the number of times it was reported
    quorum_clocks: QuorumClocks,
    // `shard_commits` is only used when commands accessed more than one shard
    shards_commits: Option<ShardsCommits>,
}

impl Info for NewtInfo {
    fn new(
        process_id: ProcessId,
        _shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        let initial_value = 0;
        Self {
            status: Status::START,
            quorum: HashSet::new(),
            cmd: None,
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            votes: Votes::new(),
            quorum_clocks: QuorumClocks::new(fast_quorum_size),
            shards_commits: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardsCommits {
    process_id: ProcessId,
    shard_count: usize,
    participants: HashSet<ProcessId>,
    max_clock: u64,
    votes: Option<Votes>,
}

impl ShardsCommits {
    fn new(process_id: ProcessId, shard_count: usize) -> Self {
        let participants = HashSet::with_capacity(shard_count);
        let max_clock = 0;
        Self {
            process_id,
            shard_count,
            participants,
            max_clock,
            votes: None,
        }
    }

    fn set_votes(&mut self, votes: Votes) {
        self.votes = Some(votes);
    }

    fn add(&mut self, from: ProcessId, clock: u64) -> bool {
        assert!(self.participants.insert(from));
        self.max_clock = std::cmp::max(self.max_clock, clock);
        log!(
            "p{}: ShardsCommits:add {} {} | current max = {} | participants = {:?} | shard count = {}",
            self.process_id,
            from,
            clock,
            self.max_clock,
            self.participants,
            self.shard_count
        );

        // we're done once we have received a message from each shard
        self.participants.len() == self.shard_count
    }
}

// `Newt` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    // Protocol messages
    MCollect {
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        clock: u64,
        coordinator_votes: Votes,
    },
    MCollectAck {
        dot: Dot,
        clock: u64,
        process_votes: Votes,
    },
    MCommit {
        dot: Dot,
        clock: u64,
        votes: Votes,
    },
    MCommitClock {
        clock: u64,
    },
    MDetached {
        detached: Votes,
    },
    MConsensus {
        dot: Dot,
        ballot: u64,
        clock: ConsensusValue,
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
        clock: u64,
    },
    MShardAggregatedCommit {
        dot: Dot,
        clock: u64,
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

const CLOCK_BUMP_WORKER_INDEX: usize = 1;

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{
            worker_dot_index_shift, worker_index_no_shift, GC_WORKER_INDEX,
        };
        debug_assert_eq!(GC_WORKER_INDEX, 0);

        match self {
            // Protocol messages
            Self::MCollect { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCollectAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommitClock { .. } => {
                worker_index_no_shift(CLOCK_BUMP_WORKER_INDEX)
            }
            Self::MDetached { .. } => {
                worker_index_no_shift(CLOCK_BUMP_WORKER_INDEX)
            }
            Self::MConsensus { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensusAck { dot, .. } => worker_dot_index_shift(&dot),
            // Partial replication messages
            Self::MForwardSubmit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MShardCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MShardAggregatedCommit { dot, .. } => {
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
    ClockBump,
}

impl PeriodicEventIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{worker_index_no_shift, GC_WORKER_INDEX};
        debug_assert_eq!(GC_WORKER_INDEX, 0);

        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
            Self::ClockBump => worker_index_no_shift(CLOCK_BUMP_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    START,
    PENDING,
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
    use fantoch::util;

    #[test]
    fn sequential_newt_test() {
        newt_flow::<SequentialKeyClocks>();
    }

    #[test]
    fn atomic_newt_test() {
        newt_flow::<AtomicKeyClocks>();
    }

    fn newt_flow<KC: KeyClocks>() {
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
        let mut config = Config::new(n, f);
        // set tiny quorums to false so that the "skip mcollect ack optimization
        // doesn't kick in"
        config.set_newt_tiny_quorums(false);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // executors
        let executors = 1;
        let executor_1 =
            TableExecutor::new(process_id_1, shard_id, config, executors);
        let executor_2 =
            TableExecutor::new(process_id_2, shard_id, config, executors);
        let executor_3 =
            TableExecutor::new(process_id_3, shard_id, config, executors);

        // newts
        let (mut newt_1, _) = Newt::<KC>::new(process_id_1, shard_id, config);
        let (mut newt_2, _) = Newt::<KC>::new(process_id_2, shard_id, config);
        let (mut newt_3, _) = Newt::<KC>::new(process_id_3, shard_id, config);

        // discover processes in all newts
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        newt_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        newt_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        newt_3.discover(sorted);

        // register processes
        simulation.register_process(newt_1, executor_1);
        simulation.register_process(newt_2, executor_2);
        simulation.register_process(newt_3, executor_3);

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

        // create client 1 that is connected to newt 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, workload);

        // discover processes in client 1
        let sorted = util::sort_processes_by_distance(
            &client_region,
            &planet,
            processes,
        );
        client_1.discover(sorted);

        // start client
        let (target_shard, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is newt 1
        assert_eq!(target, process_id_1);

        // register clients
        simulation.register_client(client_1);

        // register command in executor and submit it in newt 1
        let (process, executor, time) = simulation.get_process(target);
        executor.wait_for(&cmd);
        let mut actions = process.submit(None, cmd, time);
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
            matches!(mcommit.clone(), (_, Action::ToSend { target, .. }) if check_target(&target))
        );

        // all processes handle it
        let actions = simulation.forward_to_processes(mcommit);
        // there are four actions
        assert_eq!(actions.len(), 4);

        // we have 3 MCommitDots and one MDetached (by the process that's not
        // part of the fast quorum)
        let mut mcommitdot_count = 0;
        let mut mdetached_count = 0;
        actions.into_iter().for_each(|(_, action)| match action {
            Action::ToForward { msg } => {
                assert!(matches!(msg, Message::MCommitDot {..}));
                mcommitdot_count += 1;
            }
            Action::ToSend { msg, .. } => {
                assert!(matches!(msg, Message::MDetached {..}));
                mdetached_count += 1;
            }
        });
        assert_eq!(mcommitdot_count, 3);
        assert_eq!(mdetached_count, 1);

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
