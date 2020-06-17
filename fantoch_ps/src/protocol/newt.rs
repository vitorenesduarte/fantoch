use crate::executor::TableExecutor;
use crate::protocol::common::synod::{Synod, SynodMessage};
use crate::protocol::common::table::{
    AtomicKeyClocks, KeyClocks, QuorumClocks, SequentialKeyClocks, Votes,
};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId};
use fantoch::protocol::{
    Action, BaseProcess, CommandsInfo, Info, MessageIndex, PeriodicEventIndex,
    Protocol, ProtocolMetrics,
};
use fantoch::time::SysTime;
use fantoch::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use threshold::VClock;
use tracing::instrument;

pub type NewtSequential = Newt<SequentialKeyClocks>;
pub type NewtAtomic = Newt<AtomicKeyClocks>;

type ExecutionInfo = <TableExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone)]
pub struct Newt<KC> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: CommandsInfo<NewtInfo>,
    to_executor: Vec<ExecutionInfo>,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, u64, Votes)>,
    skip_fast_ack: bool,
}

impl<KC: KeyClocks> Protocol for Newt<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = TableExecutor;

    /// Creates a new `Newt` process.
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, u64)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size, _) =
            config.newt_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_clocks = KC::new(process_id);
        let cmds = CommandsInfo::new(
            process_id,
            config.n(),
            config.f(),
            fast_quorum_size,
        );
        let to_executor = Vec::new();
        let buffered_commits = HashMap::new();
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
            skip_fast_ack,
        };

        // maybe create garbage collection periodic event
        let mut events =
            if let Some(gc_delay) = config.garbage_collection_interval() {
                vec![(PeriodicEvent::GarbageCollection, gc_delay as u64)]
            } else {
                vec![]
            };

        // maybe create clock bump periodic event
        if config.newt_real_time() {
            let clock_bump_interval = config.newt_clock_bump_interval() as u64;
            events.reserve_exact(1);
            events.push((PeriodicEvent::ClockBump, clock_bump_interval));
        }

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
    ) -> Vec<Action<Self>> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

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

        // send votes if we can bypass the mcollectack, otherwise store them
        let coordinator_votes = if self.skip_fast_ack {
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

        // return `ToSend`
        vec![Action::ToSend {
            target,
            msg: mcollect,
        }]
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
            "p{}: MCollect({:?}, {:?}, {}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            votes,
            from,
            time.now()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return vec![];
        }

        // check if part of fast quorum
        if !quorum.contains(&self.bp.process_id) {
            if self.bp.config.newt_real_time() {
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
            assert_eq!(process_votes.len(), cmd.key_count());
            (clock, process_votes)
        };

        // update command info
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        // set consensus value
        assert!(info.synod.set_if_not_accepted(|| clock));

        if !message_from_self && self.skip_fast_ack {
            votes.merge(process_votes);

            // if tiny quorums and f = 1, the fast quorum process can commit the
            // command right away create `MCommit` and target
            let mcommit = Message::MCommit { dot, clock, votes };
            let target = self.bp.all();

            // return `ToSend`
            vec![Action::ToSend {
                target,
                msg: mcommit,
            }]
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

    #[instrument(skip(self, from, dot, clock, remote_votes, time))]
    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        remote_votes: Votes,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCollectAck({:?}, {}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            remote_votes,
            from,
            time.now()
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

                // create `MCommit` and target
                let mcommit = Message::MCommit {
                    dot,
                    clock: max_clock,
                    votes,
                };
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

    #[instrument(skip(self, from, dot, clock, time))]
    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        mut votes: Votes,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MCommit({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            votes,
            time.now()
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
        let execution_info = cmd.into_iter().map(|(key, op)| {
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
            if self.bp.config.newt_real_time() {
                vec![]
            } else {
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

    #[instrument(skip(self, detached, time))]
    fn handle_mdetached(
        &mut self,
        detached: Votes,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MDetached({:?}) | time={}",
            self.id(),
            detached,
            time.now()
        );

        // create execution info
        let execution_info = detached.into_iter().map(|(key, key_votes)| {
            ExecutionInfo::detached_votes(key, key_votes)
        });
        self.to_executor.extend(execution_info);

        // nothing to send
        vec![]
    }

    #[instrument(skip(self, from, dot, ballot, clock, time))]
    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        clock: ConsensusValue,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>> {
        log!(
            "p{}: MConsensus({:?}, {}, {:?}) | time={}",
            self.id(),
            dot,
            ballot,
            clock,
            time.now()
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
            time.now()
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

                // enough accepts were gathered and the value has been chosen: create `MCommit` and target
                let target = self.bp.all();
                let mcommit = Message::MCommit { dot, clock, votes };

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
            time.now()
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
            time.now()
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
            time.now()
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
            time.now()
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

        // iterate all clocks and bump them to the current time:
        // - TODO: only bump the clocks of active keys (i.e. keys with an
        //   `MCollect` without the  corresponding `MCommit`)
        let detached = self.key_clocks.vote_all(time.now());

        // create `ToSend`
        vec![Action::ToSend {
            target: self.bp.all(),
            msg: Message::MDetached { detached },
        }]
    }

    // Replaces the value `local_votes` with empty votes, returning the previous
    // votes.
    fn reset_votes(local_votes: &mut Votes) -> Votes {
        mem::take(local_votes)
    }

    fn gc_running(&self) -> bool {
        self.bp.config.garbage_collection_interval().is_some()
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
#[derive(Debug, Clone)]
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
}

impl Info for NewtInfo {
    fn new(
        process_id: ProcessId,
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
        }
    }
}

// `Newt` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
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
#[derive(Debug, PartialEq, Clone)]
enum Status {
    START,
    PENDING,
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, Workload};
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
        let mut config = Config::new(n, f);
        // set tiny quorums to false so that the "skip mcollect ack optimization
        // doesn't kick in"
        config.set_newt_tiny_quorums(false);

        // make sure stability is running
        config.set_garbage_collection_interval(100);

        // executors
        let executor_1 = TableExecutor::new(process_id_1, config);
        let executor_2 = TableExecutor::new(process_id_2, config);
        let executor_3 = TableExecutor::new(process_id_3, config);

        // newts
        let (mut newt_1, _) = Newt::<KC>::new(process_id_1, config);
        let (mut newt_2, _) = Newt::<KC>::new(process_id_2, config);
        let (mut newt_3, _) = Newt::<KC>::new(process_id_3, config);

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
        let conflict_rate = 100;
        let total_commands = 10;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

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
        assert!(client_1.discover(sorted));

        // start client
        let (target, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");

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
        let check_target = |target: &HashSet<u64>| target.len() == n;
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
        let check_target = |target: &HashSet<u64>| target.len() == n;
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
