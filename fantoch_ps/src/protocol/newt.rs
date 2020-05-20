use crate::executor::TableExecutor;
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
use fantoch::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;
use threshold::VClock;

pub type NewtSequential = Newt<SequentialKeyClocks>;
pub type NewtAtomic = Newt<AtomicKeyClocks>;

type ExecutionInfo = <TableExecutor as Executor>::ExecutionInfo;

#[derive(Clone)]
pub struct Newt<KC> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: CommandsInfo<NewtInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl<KC: KeyClocks> Protocol for Newt<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = TableExecutor;

    /// Creates a new `Newt` process.
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, usize)>) {
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

        // create `Newt`
        let protocol = Self {
            bp,
            key_clocks,
            cmds,
            to_executor,
        };

        // create periodic events
        let gc_delay = config.garbage_collection_interval();
        let events = vec![(PeriodicEvent::GarbageCollection, gc_delay)];

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
    fn submit(&mut self, dot: Option<Dot>, cmd: Command) -> Action<Message> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
    ) -> Action<Message> {
        match msg {
            Message::MCollect {
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock),
            Message::MCollectAck {
                dot,
                clock,
                process_votes,
            } => self.handle_mcollectack(from, dot, clock, process_votes),
            Message::MCommit {
                dot,
                cmd,
                clock,
                votes,
            } => self.handle_mcommit(dot, cmd, clock, votes),
            Message::MCommitDot { dot } => self.handle_mcommit_dot(from, dot),
            Message::MPhantom { dot, process_votes } => {
                self.handle_mphantom(dot, process_votes)
            }
            Message::MGarbageCollection { committed } => {
                self.handle_mgc(from, committed)
            }
            Message::MStable { stable } => self.handle_mstable(from, stable),
        }
    }

    /// Handles periodic local events.
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
    ) -> Vec<Action<Message>> {
        match event {
            PeriodicEvent::GarbageCollection => {
                log!("p{}: PeriodicEvent::GarbageCollection", self.id());
                // retrieve the committed clock and stable dots
                let (committed, stable) = self.cmds.committed_and_stable();

                // create `ToSend`
                let tosend = Action::ToSend {
                    target: self.bp.all_but_me(),
                    msg: Message::MGarbageCollection { committed },
                };

                // create `ToForward` to self
                let toforward = Action::ToForward {
                    msg: Message::MStable { stable },
                };

                vec![tosend, toforward]
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

impl<KC: KeyClocks> Newt<KC> {
    /// Handles a submit operation by a client.
    fn handle_submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
    ) -> Action<Message> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // compute its clock:
        // - this may also consume votes since we're bumping the clocks here
        // - for that reason, we'll store these votes locally and not recompute
        //   them once we receive the `MCollect` from self
        let (clock, process_votes) = self.key_clocks.bump_and_vote(&cmd, 0);

        // get cmd info
        let info = self.cmds.get(dot);
        // bootstrap votes with initial consumed votes
        info.votes = process_votes;

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        Action::ToSend {
            target,
            msg: mcollect,
        }
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        remote_clock: u64,
    ) -> Action<Message> {
        log!(
            "p{}: MCollect({:?}, {:?}, {}) from {}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return Action::Nothing;
        }

        // check if it's a message from self
        let message_from_self = from == self.bp.process_id;

        let (clock, process_votes) = if message_from_self {
            // if it is, do not recompute clock and votes
            (remote_clock, Votes::new(None))
        } else {
            // otherwise, compute clock considering the `remote_clock` as its
            // minimum value
            let (clock, process_votes) =
                self.key_clocks.bump_and_vote(&cmd, remote_clock);
            // check that there's one vote per key
            assert_eq!(process_votes.len(), cmd.key_count());
            (clock, process_votes)
        };

        // update command info
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        info.clock = clock;

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck {
            dot,
            clock,
            process_votes,
        };
        let target = singleton![from];

        // return `ToSend`
        Action::ToSend {
            target,
            msg: mcollectack,
        }
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        remote_votes: Votes,
    ) -> Action<Message> {
        log!(
            "p{}: MCollectAck({:?}, {}, {:?}) from {}",
            self.id(),
            dot,
            clock,
            remote_votes,
            from
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status != Status::COLLECT {
            // do nothing if we're no longer COLLECT
            return Action::Nothing;
        }

        // update votes with remote votes
        info.votes.merge(remote_votes);

        // update quorum clocks while computing max clock and its number of
        // occurences
        let (max_clock, max_count) = info.quorum_clocks.add(from, clock);

        // optimization: bump all keys clocks in `cmd` to be `max_clock`
        // - this prevents us from generating votes (either when clients submit
        //   new operations or when handling `MCollect` from other processes)
        //   that could potentially delay the execution of this command
        match info.cmd.as_ref() {
            Some(cmd) => {
                let local_votes = self.key_clocks.vote(cmd, max_clock);
                // update votes with local votes
                info.votes.merge(local_votes);
            }
            None => {
                panic!("there should be a command payload in the MCollectAck handler");
            }
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
                // TODO create a slim-MCommit that only sends the payload to the
                // non-fast-quorum members, or send the payload
                // to all in a slim-MConsensus
                let mcommit = Message::MCommit {
                    dot,
                    cmd: info.cmd.clone(),
                    clock: max_clock,
                    votes,
                };
                let target = self.bp.all();

                // return `ToSend`
                Action::ToSend {
                    target,
                    msg: mcommit,
                }
            } else {
                self.bp.slow_path();
                // TODO slow path
                todo!("slow path not implemented yet")
            }
        } else {
            Action::Nothing
        }
    }

    fn handle_mcommit(
        &mut self,
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        mut votes: Votes,
    ) -> Action<Message> {
        log!("p{}: MCommit({:?}, {}, {:?})", self.id(), dot, clock, votes);

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return Action::Nothing;
        }

        // update command info:
        info.status = Status::COMMIT;
        info.cmd = cmd;
        info.clock = clock;

        // get current votes (probably from phantom messages) merge them with
        // received votes so that all together can be added to a votes
        // table
        let current_votes = Self::reset_votes(&mut info.votes);
        votes.merge(current_votes);

        // generate phantom votes if committed clock is higher than the local
        // key's clock:
        // - not all processes are needed for stability specially when newt is
        //   *not* configured with tiny quorums
        // - so in case it's not, only the processes part of the fast quorum (if
        //   it was, `info.quorum` is not empty) generate phantoms
        // - n = 3 is a special case  where phantom votes are not generated as
        //   they are not needed
        let mut to_send = Action::Nothing;
        if self.bp.config.n() > 3
            && (self.bp.config.newt_tiny_quorums() || !info.quorum.is_empty())
        {
            if let Some(cmd) = info.cmd.as_ref() {
                // if not a no op, check if we can generate more votes that can
                // speed-up execution
                let process_votes = self.key_clocks.vote(cmd, info.clock);

                // create `MPhantom` if there are new votes
                if !process_votes.is_empty() {
                    let mphantom = Message::MPhantom { dot, process_votes };
                    let target = self.bp.all();
                    to_send = Action::ToSend {
                        target,
                        msg: mphantom,
                    };
                }
            }
        }

        // create execution info if not a noop
        if let Some(cmd) = info.cmd.clone() {
            // create execution info
            let rifl = cmd.rifl();
            let execution_info = cmd.into_iter().map(|(key, op)| {
                // find votes on this key
                let key_votes = votes
                    .remove(&key)
                    .expect("there should be votes on all command keys");
                ExecutionInfo::votes(dot, info.clock, rifl, key, op, key_votes)
            });
            self.to_executor.extend(execution_info);
        } else {
            // TODO if noOp, we should add `Votes` to all tables
            panic!("noOp votes should be broadcast to all executors");
        }

        // record that this command has been committed
        self.cmds.commit(dot);

        // return `ToSend`
        to_send
    }

    fn handle_mphantom(&mut self, dot: Dot, votes: Votes) -> Action<Message> {
        log!("p{}: MPhantom({:?}, {:?})", self.id(), dot, votes);

        // get cmd info
        let info = self.cmds.get(dot);

        // TODO if there's ever a Status::EXECUTE, this check might be incorrect
        if info.status == Status::COMMIT {
            // create execution info
            let execution_info = votes.into_iter().map(|(key, key_votes)| {
                ExecutionInfo::phantom_votes(key, key_votes)
            });
            self.to_executor.extend(execution_info);
        } else {
            // if not committed yet, update votes with remote votes
            info.votes.merge(votes);
        }

        // nothing to send
        Action::Nothing
    }

    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
    ) -> Action<Message> {
        log!("p{}: MCommitDot({:?})", self.id(), dot);
        assert_eq!(from, self.bp.process_id);
        self.cmds.commit(dot);
        Action::Nothing
    }

    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
    ) -> Action<Message> {
        log!(
            "p{}: MGarbageCollection({:?}) from {}",
            self.id(),
            committed,
            from
        );
        self.cmds.committed_by(from, committed);
        Action::Nothing
    }

    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
    ) -> Action<Message> {
        log!("p{}: MStable({:?}) from {}", self.id(), stable, from);
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
        Action::Nothing
    }

    // Replaces the value `local_votes` with empty votes, returning the previous
    // votes.
    fn reset_votes(local_votes: &mut Votes) -> Votes {
        mem::take(local_votes)
    }
}

// `NewtInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Clone)]
struct NewtInfo {
    status: Status,
    quorum: HashSet<ProcessId>,
    cmd: Option<Command>, // `None` if noOp
    clock: u64,
    // `votes` is used by the coordinator to aggregate `ProcessVotes` from fast
    // quorum members
    votes: Votes,
    // `quorum_clocks` is used by the coordinator to compute the highest clock
    // reported by fast quorum members and the number of times it was reported
    quorum_clocks: QuorumClocks,
}

impl Info for NewtInfo {
    fn new(_: ProcessId, _: usize, _: usize, fast_quorum_size: usize) -> Self {
        Self {
            status: Status::START,
            quorum: HashSet::new(),
            cmd: None,
            clock: 0,
            votes: Votes::new(None),
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
    },
    MCollectAck {
        dot: Dot,
        clock: u64,
        process_votes: Votes,
    },
    MCommit {
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        votes: Votes,
    },
    MPhantom {
        dot: Dot,
        process_votes: Votes,
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
            dot_worker_index_reserve, no_worker_index_reserve, GC_WORKER_INDEX,
        };
        match self {
            // Protocol messages
            Self::MCollect { dot, .. } => dot_worker_index_reserve(&dot),
            Self::MCollectAck { dot, .. } => dot_worker_index_reserve(&dot),
            Self::MCommit { dot, .. } => dot_worker_index_reserve(&dot),
            Self::MPhantom { dot, .. } => dot_worker_index_reserve(&dot),
            // GC messages
            Self::MCommitDot { .. } => no_worker_index_reserve(GC_WORKER_INDEX),
            Self::MGarbageCollection { .. } => {
                no_worker_index_reserve(GC_WORKER_INDEX)
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
        use fantoch::run::{no_worker_index_reserve, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => no_worker_index_reserve(GC_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(PartialEq, Clone)]
enum Status {
    START,
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
        // set tiny quorums to true:
        // - doesn't change the fast quorum size for n = 3 and f = 1 but
        // - if affects phantom vote generation
        config.set_newt_tiny_quorums(true);

        // executors
        let executor_1 = TableExecutor::new(config);
        let executor_2 = TableExecutor::new(config);
        let executor_3 = TableExecutor::new(config);

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
        let (process, executor) = simulation.get_process(target);
        executor.wait_for(&cmd);
        let mcollect = process.submit(None, cmd);

        // check that the mcollect is being sent to 2 processes
        let check_target = |target: &HashSet<u64>| {
            target.len() == 2 * f && target.contains(&1) && target.contains(&2)
        };
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
        let to_sends = simulation.forward_to_processes(mcommit);
        // there should be nothing to send
        assert!(to_sends.is_empty());

        // process 1 should have something to the executor
        let (process, executor) = simulation.get_process(process_id_1);
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
            .forward_to_client(cmd_result, &time)
            .expect("there should a new submit");

        let (process, _) = simulation.get_process(target);
        let action = process.submit(None, cmd);
        let check_msg = |msg: &Message| matches!(msg, Message::MCollect {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(matches!(action, Action::ToSend {msg, ..} if check_msg(&msg)));
    }
}
