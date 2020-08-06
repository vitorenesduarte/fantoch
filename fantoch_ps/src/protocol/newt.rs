use crate::executor::TableExecutor;
use crate::protocol::common::synod::{Synod, SynodMessage};
use crate::protocol::common::table::{
    AtomicKeyClocks, FineLockedKeyClocks, KeyClocks, LockedKeyClocks,
    QuorumClocks, SequentialKeyClocks, Votes,
};
use crate::protocol::partial::{self, ShardsCommits};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId, ShardId};
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

pub type NewtSequential = Newt<SequentialKeyClocks>;
pub type NewtAtomic = Newt<AtomicKeyClocks>;
pub type NewtLocked = Newt<LockedKeyClocks>;
pub type NewtFineLocked = Newt<FineLockedKeyClocks>;

type ExecutionInfo = <TableExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Newt<KC: KeyClocks> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: CommandsInfo<NewtInfo>,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
    // set of detached votes
    detached: Votes,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, u64, Votes)>,
    // bump to messages that arrived before the initial `MCollect` message
    buffered_bump_tos: HashMap<Dot, u64>,
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
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let detached = Votes::new();
        let buffered_commits = HashMap::new();
        let buffered_bump_tos = HashMap::new();
        // enable skip fast ack if configured like that and the fast quorum size
        // is 2
        let skip_fast_ack = config.skip_fast_ack() && fast_quorum_size == 2;

        // create `Newt`
        let protocol = Self {
            bp,
            key_clocks,
            cmds,
            to_processes,
            to_executors,
            detached,
            buffered_commits,
            buffered_bump_tos,
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

        // maybe create send detached periodic event
        if let Some(interval) = config.newt_detached_send_interval() {
            events.reserve_exact(1);
            events.push((PeriodicEvent::SendDetached, interval));
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
    fn submit(&mut self, dot: Option<Dot>, cmd: Command, _time: &dyn SysTime) {
        self.handle_submit(dot, cmd, true);
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
            Message::MBumpTo { dot, clock } => {
                self.handle_mbump_to(dot, clock, time)
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
    fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime) {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
            PeriodicEvent::ClockBump => self.handle_event_clock_bump(time),
            PeriodicEvent::SendDetached => {
                self.handle_event_send_detached(time)
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

impl<KC: KeyClocks> Newt<KC> {
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
        //   disabled; this is because the `MShardCommit` messages "need to" be
        //   aggregated at a single process (in order to reduce net traffic)
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

        // add `MCollect` send as action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollect,
        });
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
    ) {
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
            return;
        }

        // check if part of fast quorum
        if !quorum.contains(&self.bp.process_id) {
            // if not:
            // - maybe initialize `self.key_clocks`
            // - simply save the payload and set status to `PAYLOAD`
            // - if we received the `MCommit` before the `MCollect`, handle the
            //   `MCommit` now

            if self.bp.config.newt_clock_bump_interval().is_some() {
                // make sure there's a clock for each existing key:
                // - this ensures that all clocks will be bumped in the periodic
                //   clock bump event
                self.key_clocks.init_clocks(&cmd);
            }

            info.status = Status::PAYLOAD;
            info.cmd = Some(cmd);

            // check if there's a buffered commit notification; if yes, handle
            // the commit again (since now we have the payload)
            if let Some((from, clock, votes)) =
                self.buffered_commits.remove(&dot)
            {
                self.handle_mcommit(from, dot, clock, votes, time);
            }
            return;
        }

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;
        let (clock, process_votes) = if message_from_self {
            // if it is a message from self, do not recompute clock and votes
            (remote_clock, Votes::new())
        } else {
            // if not from self, compute clock considering `remote_clock` as the
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

        // if there are any buffered `MBumpTo`'s, generate detached votes
        if let Some(bump_to) = self.buffered_bump_tos.remove(&dot) {
            self.key_clocks.vote(&cmd, bump_to, &mut self.detached);
        }

        // get shard count
        let shard_count = cmd.shard_count();

        // update command info
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        // set consensus value
        assert!(info.synod.set_if_not_accepted(|| clock));

        // (see previous use of `self.skip_fast_ack` for an explanation of
        // what's going on here)
        if !message_from_self && self.skip_fast_ack && shard_count == 1 {
            votes.merge(process_votes);

            // if tiny quorums and f = 1, the fast quorum process can commit the
            // command right away; create `MCommit`
            Self::mcommit_actions(
                &self.bp,
                info,
                shard_count,
                dot,
                clock,
                votes,
                &mut self.to_processes,
            )
        } else {
            self.mcollect_actions(from, dot, clock, process_votes, shard_count)
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
    ) {
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
            return;
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
        //   with real time, otherwise there's a huge tail; but that doesn't
        //   make any sense; NOTE: this was probably before high-resolution
        //   real-time clocks
        let cmd = info.cmd.as_ref().unwrap();
        if !message_from_self {
            self.key_clocks.vote(cmd, max_clock, &mut self.detached);
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
                let shard_count = cmd.shard_count();
                Self::mcommit_actions(
                    &self.bp,
                    info,
                    shard_count,
                    dot,
                    max_clock,
                    votes,
                    &mut self.to_processes,
                )
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
                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mconsensus,
                })
            }
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
    ) {
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
            return;
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return;
        }

        // create execution info
        let cmd = info.cmd.clone().expect("there should be a command payload");
        let rifl = cmd.rifl();
        let execution_info =
            cmd.into_iter(self.bp.shard_id).map(|(key, op)| {
                // find votes on this key
                let key_votes = votes
                    .remove(&key)
                    .expect("there should be votes on all command keys");
                ExecutionInfo::votes(dot, clock, rifl, key, op, key_votes)
            });
        self.to_executors.extend(execution_info);

        // update command info:
        info.status = Status::COMMIT;

        // handle commit in synod
        let msg = SynodMessage::MChosen(clock);
        assert!(info.synod.handle(from, msg).is_none());

        // don't try to generate detached votes if configured with real time
        // (since it will be done in a periodic event)
        if self.bp.config.newt_clock_bump_interval().is_none() {
            let cmd = info.cmd.as_ref().unwrap();
            self.key_clocks.vote(cmd, clock, &mut self.detached);
        }

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

    #[instrument(skip(self, dot, clock, _time))]
    fn handle_mbump_to(&mut self, dot: Dot, clock: u64, _time: &dyn SysTime) {
        log!(
            "p{}: MBumpTo({:?}, {}) | time={}",
            self.id(),
            dot,
            clock,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // maybe bump up to `clock`
        if let Some(cmd) = info.cmd.as_ref() {
            // we have the payload, thus we can bump to `clock`
            self.key_clocks.vote(cmd, clock, &mut self.detached);
        } else {
            // in this case we don't have the payload (which means we have
            // received the `MBumpTo` from some shard before `MCollect` from my
            // shard); thus, buffer this request and handle it when we do
            // receive the `MCollect` (see `handle_mcollect`)
            let current = self.buffered_bump_tos.entry(dot).or_default();
            // if the command acesses more than two shards, we could receive
            // several `MBumpTo`'s before the `MCollect`; in this case, save the
            // highest one
            *current = std::cmp::max(*current, clock);
        }
    }

    #[instrument(skip(self, detached, _time))]
    fn handle_mdetached(&mut self, detached: Votes, _time: &dyn SysTime) {
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
        self.to_executors.extend(execution_info);
    }

    #[instrument(skip(self, from, dot, ballot, clock, _time))]
    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        clock: ConsensusValue,
        _time: &dyn SysTime,
    ) {
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
                Self::mcommit_actions(&self.bp, info, shard_count, dot, clock, votes, &mut self.to_processes)
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
        clock: u64,
        _time: &dyn SysTime,
    ) {
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

        let shard_count = info.cmd.as_ref().unwrap().shard_count();
        let add_shards_commits_info =
            |shards_commit_info: &mut ShardsCommitsInfo, clock| {
                shards_commit_info.add(clock)
            };
        let create_mshard_aggregated_commit =
            |dot, shards_commit_info: &ShardsCommitsInfo| {
                Message::MShardAggregatedCommit {
                    dot,
                    clock: shards_commit_info.max_clock,
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
        clock: u64,
        _time: &dyn SysTime,
    ) {
        log!(
            "p{}: MShardAggregatedCommit({:?}, {}) | time={}",
            self.id(),
            dot,
            clock,
            _time.millis()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        let extract_mcommit_extra_data =
            |shards_commit_info: ShardsCommitsInfo| {
                shards_commit_info
                    .votes
                    .expect("votes in shard commit info should be set")
            };
        let create_mcommit =
            |dot, clock, votes| Message::MCommit { dot, clock, votes };

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
            _time.millis()
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
            _time.millis()
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
            _time.millis()
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
            _time.millis()
        );

        // retrieve the committed clock
        let committed = self.cmds.committed();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    #[instrument(skip(self, time))]
    fn handle_event_clock_bump(&mut self, time: &dyn SysTime) {
        log!(
            "p{}: PeriodicEvent::ClockBump | time={}",
            self.id(),
            time.millis()
        );

        // Iterate all clocks and bump them to the current time. The fact that
        // micros as used here is not an accident. With many
        // clients (like 1024 per site with 5 sites), millis do not provide
        // "enough precision".
        // - TODO: only bump the clocks of active keys (i.e. keys with an
        //   `MCollect` without the  corresponding `MCommit`)
        self.key_clocks.vote_all(time.micros(), &mut self.detached);
    }

    #[instrument(skip(self, _time))]
    fn handle_event_send_detached(&mut self, _time: &dyn SysTime) {
        log!(
            "p{}: PeriodicEvent::SendDetached | time={}",
            self.id(),
            _time.millis()
        );

        let detached = mem::take(&mut self.detached);
        if !detached.is_empty() {
            self.to_processes.push(Action::ToSend {
                target: self.bp.all(),
                msg: Message::MDetached { detached },
            });
        }
    }

    // if the command accesses more than one shard, notify the remaining shards
    // so that they can bump the keys accessed by this command on their shard to
    // the timestamp computed here
    fn mcollect_actions(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        process_votes: Votes,
        shard_count: usize,
    ) {
        // create `MCollectAck`
        let mcollectack = Message::MCollectAck {
            dot,
            clock,
            process_votes,
        };
        let target = singleton![from];
        self.to_processes.push(Action::ToSend {
            msg: mcollectack,
            target,
        });

        if shard_count > 1 {
            // get cmd info
            let info = self.cmds.get(dot);
            let cmd = info.cmd.as_ref().unwrap();

            // create bumpto messages if the command acesses more than one shard
            let my_shard_id = self.bp.shard_id;
            for shard_id in
                cmd.shards().filter(|shard_id| **shard_id != my_shard_id)
            {
                let mbump_to = Message::MBumpTo { dot, clock };
                let target =
                    singleton![self.bp.closest_shard_process(shard_id)];
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mbump_to,
                })
            }
        }
    }

    fn mcommit_actions(
        bp: &BaseProcess,
        info: &mut NewtInfo,
        shard_count: usize,
        dot: Dot,
        clock: u64,
        votes: Votes,
        to_processes: &mut Vec<Action<Self>>,
    ) {
        let create_mcommit =
            |dot, clock, votes| Message::MCommit { dot, clock, votes };
        let create_mshard_commit =
            |dot, clock| Message::MShardCommit { dot, clock };
        let update_shards_commit_info =
            |shards_commits_info: &mut ShardsCommitsInfo, votes| {
                shards_commits_info.set_votes(votes)
            };

        partial::mcommit_actions(
            bp,
            &mut info.shards_commits,
            shard_count,
            dot,
            clock,
            votes,
            create_mcommit,
            create_mshard_commit,
            update_shards_commit_info,
            to_processes,
        )
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
    // `None` if not set yet
    cmd: Option<Command>,
    // `votes` is used by the coordinator to aggregate `ProcessVotes` from fast
    // quorum members
    votes: Votes,
    // `quorum_clocks` is used by the coordinator to compute the highest clock
    // reported by fast quorum members and the number of times it was reported
    quorum_clocks: QuorumClocks,
    // `shard_commits` is only used when commands accessed more than one shard
    shards_commits: Option<ShardsCommits<ShardsCommitsInfo>>,
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

#[derive(Default, Debug, Clone, PartialEq, Eq)]
struct ShardsCommitsInfo {
    max_clock: u64,
    votes: Option<Votes>,
}

impl ShardsCommitsInfo {
    fn add(&mut self, clock: u64) {
        self.max_clock = std::cmp::max(self.max_clock, clock);
    }

    fn set_votes(&mut self, votes: Votes) {
        self.votes = Some(votes);
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
    MBumpTo {
        dot: Dot,
        clock: u64,
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
            Self::MDetached { .. } => {
                worker_index_no_shift(CLOCK_BUMP_WORKER_INDEX)
            }
            Self::MConsensus { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensusAck { dot, .. } => worker_dot_index_shift(&dot),
            // Partial replication messages
            Self::MForwardSubmit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MBumpTo { dot, .. } => worker_dot_index_shift(&dot),
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
    SendDetached,
}

impl PeriodicEventIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{worker_index_no_shift, GC_WORKER_INDEX};
        debug_assert_eq!(GC_WORKER_INDEX, 0);

        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
            Self::ClockBump => worker_index_no_shift(CLOCK_BUMP_WORKER_INDEX),
            Self::SendDetached => {
                // should be sent to all workers
                None
            }
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
        let closest =
            util::closest_process_per_shard(&client_region, &planet, processes);
        client_1.connect(closest);

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
            matches!(mcommit.clone(), (_, Action::ToSend { target, .. }) if check_target(&target))
        );

        // all processes handle it
        let actions = simulation.forward_to_processes(mcommit);
        // there are three actions
        assert_eq!(actions.len(), 3);

        // check that the three actions are the forward of MCommitDot
        assert!(actions.into_iter().all(|(_, action)| {
            if let Action::ToForward { msg } = action {
                matches!(msg, Message::MCommitDot {..})
            } else {
                false
            }
        }));

        // process 1 should have something to the executor
        let (process, executor, _) = simulation.get_process(process_id_1);
        let to_executor: Vec<_> = process.to_executors_iter().collect();
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
