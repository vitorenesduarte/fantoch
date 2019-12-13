// This module contains the definition of `ProcessVotes`, `Votes` and `VoteRange`.
mod votes;

// This module contains the definition of `MultiVotesTable`.
mod votes_table;

// This module contains the definition of `KeyClocks` and `QuorumClocks`.
mod clocks;

use crate::command::{Command, CommandResult, Pending};
use crate::config::Config;
use crate::id::{Dot, ProcessId, Rifl};
use crate::kvs::{KVOp, KVStore, Key};
use crate::planet::{Planet, Region};
use crate::protocol::newt::clocks::{KeysClocks, QuorumClocks};
use crate::protocol::newt::votes::{ProcessVotes, Votes};
use crate::protocol::newt::votes_table::MultiVotesTable;
use crate::protocol::{BaseProcess, Process, ToSend};
use std::collections::HashMap;

pub struct Newt {
    bp: BaseProcess,
    keys_clocks: KeysClocks,
    cmds_info: CommandsInfo,
    table: MultiVotesTable,
    store: KVStore,
    pending: Pending,
}

impl Process for Newt {
    type Message = Message;

    /// Creates a new `Newt` process.
    fn new(id: ProcessId, region: Region, planet: Planet, config: Config) -> Self {
        // compute fast quorum size and stability threshold
        let q = Newt::fast_quorum_size(&config);
        let stability_threshold = Newt::stability_threshold(&config);

        // create `MultiVotesTable`
        let table = MultiVotesTable::new(config.n(), stability_threshold);

        // create `BaseProcess`, `Clocks`, dot_to_info, `KVStore` and `Pending`.
        let bp = BaseProcess::new(id, region, planet, config, q);
        let keys_clocks = KeysClocks::new(id);
        let cmds_info = CommandsInfo::new(q);
        let store = KVStore::new();
        let pending = Pending::new();

        // create `Newt`
        Self {
            bp,
            keys_clocks,
            cmds_info,
            table,
            store,
            pending,
        }
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Updates the processes known by this process.
    fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message> {
        self.handle_submit(cmd)
    }

    /// Handles protocol messages.
    fn handle(&mut self, msg: Self::Message) -> ToSend<Self::Message> {
        match msg {
            Message::MCollect {
                from,
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock),
            Message::MCollectAck {
                from,
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
        }
    }
}

impl Newt {
    /// Computes `Newt` fast quorum size.
    fn fast_quorum_size(config: &Config) -> usize {
        2 * config.f()
    }

    /// Computes `Newt` stability threshold.
    /// Typically the threshold should be n - q + 1, where n is the number of
    /// processes and q the size of the write quorum. In `Newt`, although
    /// the fast quorum is 2f (which would suggest q = 2f), in fact q = f + 1.
    /// The quorum size of 2f ensures that all clocks are computed from f + 1
    /// processes. So, n - q + 1 = n - (f + 1) + 1 = n - f
    fn stability_threshold(config: &Config) -> usize {
        config.n() - config.f()
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: Command) -> ToSend<Message> {
        // start command in `Pending`
        self.pending.start(&cmd);

        // compute the command identifier
        let dot = self.bp.next_dot();

        // compute its clock
        let clock = self.keys_clocks.clock(&cmd) + 1;

        // clone the fast quorum
        let fast_quorum = self.bp.fast_quorum.clone().unwrap();

        // create `MCollect`
        let mcollect = Message::MCollect {
            from: self.id(),
            dot,
            cmd,
            clock,
            quorum: fast_quorum.clone(),
        };

        // return `ToSend`
        ToSend::ToProcesses(fast_quorum, mcollect)
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: Vec<ProcessId>,
        clock: u64,
    ) -> ToSend<Message> {
        // get cmd info
        let info = self.cmds_info.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return ToSend::Nothing;
        }

        // TODO can we somehow combine a subset of the next 3 operations in
        // order to save HashMap operations?

        // compute command clock
        let cmd_clock = std::cmp::max(clock, self.keys_clocks.clock(&cmd) + 1);

        // compute votes consumed by this command
        // - this computation needs to occur before the next `bump_to`
        let process_votes = self.keys_clocks.process_votes(&cmd, cmd_clock);

        // if coordinator, set keys in `info.votes`
        // (`info.quorum_clocks` is initialized in `self.cmds_info.get`)
        if self.bp.process_id == dot.source() {
            info.votes.set_keys(&cmd);
        }

        // update command info:
        // - change status to collect
        // - save command and quorum
        // - set command clock
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        info.clock = cmd_clock;

        // create `MCollectAck`
        let mcollectack = Message::MCollectAck {
            from: self.bp.process_id,
            dot,
            clock: info.clock,
            process_votes,
        };

        // return `ToSend`
        ToSend::ToProcesses(vec![from], mcollectack)
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        remote_process_votes: ProcessVotes,
    ) -> ToSend<Message> {
        // get cmd info
        let info = self.cmds_info.get(dot);

        if info.status != Status::COLLECT || info.quorum_clocks.contains(from) {
            // do nothing if we're no longer COLLECT or if this is a
            // duplicated message
            return ToSend::Nothing;
        }

        // update votes with remote votes
        info.votes.add(remote_process_votes);

        // update quorum clocks while computing max clock and its number of
        // occurences
        let (max_clock, max_count) = info.quorum_clocks.add(from, clock);

        // optimization: bump all keys clocks in `cmd` to be `max_clock`
        // - this prevents us from generating votes (either when clients submit new operations or
        //   when handling `MCollect` from other processes) that could potentially delay the
        //   execution of this command
        let cmd = info.cmd.as_ref().unwrap();
        let local_process_votes = self.keys_clocks.process_votes(cmd, max_clock);

        // update votes with local votes
        info.votes.add(local_process_votes);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // fast path condition:
            // - if `max_clock` was reported by at least f processes
            if max_count >= self.bp.config.f() {
                // create `MCommit`
                let mcommit = Message::MCommit {
                    dot,
                    cmd: info.cmd.clone(),
                    clock: max_clock,
                    votes: info.votes.clone(),
                };

                // return `ToSend`
                ToSend::ToProcesses(self.bp.all_processes.clone().unwrap(), mcommit)
            } else {
                // TODO slow path
                ToSend::Nothing
            }
        } else {
            ToSend::Nothing
        }
    }

    fn handle_mcommit(
        &mut self,
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        votes: Votes,
    ) -> ToSend<Message> {
        // get cmd info
        let info = self.cmds_info.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return ToSend::Nothing;
        }

        // update command info:
        info.status = Status::COMMIT;
        info.cmd = cmd;
        info.clock = clock;

        // TODO generate phantom votes if committed clock is higher than the
        // local key's clock

        // update votes table and get commands that can be executed
        let to_execute = self
            .table
            .add(dot.source(), info.cmd.clone(), info.clock, votes);

        // execute commands
        if let Some(to_execute) = to_execute {
            let ready = self.execute(to_execute);
            // if there ready commands, forward them to clients
            if ready.is_empty() {
                ToSend::Nothing
            } else {
                ToSend::ToClients(ready)
            }
        } else {
            // no message to be sent
            ToSend::Nothing
        }
    }

    fn execute(&mut self, to_execute: Vec<(Key, Vec<(Rifl, KVOp)>)>) -> Vec<CommandResult> {
        let mut ready = Vec::new();
        for (key, ops) in to_execute {
            for (rifl, op) in ops {
                // execute op in the `KVStore`
                let op_result = self.store.execute(&key, op);

                // add partial result to `Pending`
                let cmd_result = self.pending.add_partial(rifl, key.clone(), op_result);

                if let Some(cmd_result) = cmd_result {
                    ready.push(cmd_result);
                }
            }
        }
        ready
    }
}

// `CommandsInfo` contains `CommandInfo` for each `Dot`.
struct CommandsInfo {
    q: usize,
    dot_to_info: HashMap<Dot, CommandInfo>,
}

impl CommandsInfo {
    fn new(q: usize) -> Self {
        Self {
            q,
            dot_to_info: HashMap::new(),
        }
    }

    // Returns the `CommandInfo` associated with `Dot`.
    // If no `CommandInfo` is associated, an empty `CommandInfo` is returned.
    fn get(&mut self, dot: Dot) -> &mut CommandInfo {
        // TODO the borrow checker complains if `self.q` is passed to
        // `CommandInfo::new`
        let q = self.q;
        self.dot_to_info
            .entry(dot)
            .or_insert_with(|| CommandInfo::new(q))
    }
}

// `CommandInfo` contains all information required in the life-cyle of a
// `Command`
struct CommandInfo {
    status: Status,
    quorum: Vec<ProcessId>,
    cmd: Option<Command>, // `None` if noOp
    clock: u64,
    // `votes` is used by the coordinator to aggregate `ProcessVotes` from fast
    // quorum members
    votes: Votes,
    // `quorum_clocks` is used by the coordinator to compute the highest clock
    // reported by fast quorum members and the number of times it was reported
    quorum_clocks: QuorumClocks,
}

impl CommandInfo {
    fn new(q: usize) -> Self {
        Self {
            status: Status::START,
            quorum: vec![],
            cmd: None,
            clock: 0,
            votes: Votes::new(),
            quorum_clocks: QuorumClocks::new(q),
        }
    }
}

// `Newt` protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    MCollect {
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: Vec<ProcessId>,
        clock: u64,
    },
    MCollectAck {
        from: ProcessId,
        dot: Dot,
        clock: u64,
        process_votes: ProcessVotes,
    },
    MCommit {
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        votes: Votes,
    },
}

/// `Status` of commands.
#[derive(PartialEq)]
enum Status {
    START,
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, Workload};
    use crate::sim::Router;
    use crate::time::SimTime;

    #[test]
    fn newt_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(Newt::fast_quorum_size(&config), 2);
        assert_eq!(Newt::stability_threshold(&config), 6);

        let config = Config::new(7, 2);
        assert_eq!(Newt::fast_quorum_size(&config), 4);
        assert_eq!(Newt::stability_threshold(&config), 5);
    }

    #[test]
    fn newt_flow() {
        // processes ids
        let process_id_1 = 1;
        let process_id_2 = 2;
        let process_id_3 = 3;

        // processes
        let processes = vec![
            (process_id_1, Region::new("europe-west2")),
            (process_id_2, Region::new("europe-west3")),
            (process_id_3, Region::new("europe-west4")),
        ];

        // planet
        let planet = Planet::new("latency/");

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // newts
        let mut newt_1 = Newt::new(
            process_id_1,
            Region::new("europe-west2"),
            planet.clone(),
            config,
        );
        let mut newt_2 = Newt::new(
            process_id_2,
            Region::new("europe-west3"),
            planet.clone(),
            config,
        );
        let mut newt_3 = Newt::new(
            process_id_3,
            Region::new("europe-west4"),
            planet.clone(),
            config,
        );

        // discover processes in all newts
        newt_1.discover(processes.clone());
        newt_2.discover(processes.clone());
        newt_3.discover(processes.clone());

        // create msg router
        let mut router = Router::new();

        // register processes
        router.register_process(newt_1);
        router.register_process(newt_2);
        router.register_process(newt_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // create client 1 that is connected to newt 1
        let client_id = 1;
        let client_region = Region::new("europe-west2");
        let mut client_1 = Client::new(client_id, client_region, planet.clone(), workload);

        // discover processes in client 1
        assert!(client_1.discover(processes));

        // start client
        let (target, cmd) = client_1.start(&time);

        // check that `target` is newt 1
        assert_eq!(target, process_id_1);

        // register clients
        router.register_client(client_1);

        // submit it in newt_0
        let mcollects = router.submit_to_process(target, cmd);

        // check that the mcollect is being sent to 2 processes
        assert!(mcollects.to_processes());
        if let ToSend::ToProcesses(to, _) = mcollects.clone() {
            assert_eq!(to.len(), 2 * f);
        } else {
            panic!("ToSend::ToProcesses not found!");
        }

        // handle in mcollects
        let mut mcollectacks = router.route(mcollects, &time);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);
        assert!(mcollectacks.iter().all(|to_send| to_send.to_processes()));

        // handle the first mcollectack
        let mut mcommits = router.route(mcollectacks.pop().unwrap(), &time);
        let mcommit_tosend = mcommits.pop().unwrap();
        // no mcommit yet
        assert!(mcommit_tosend.is_nothing());

        // handle the second mcollectack
        let mut mcommits = router.route(mcollectacks.pop().unwrap(), &time);
        let mcommit_tosend = mcommits.pop().unwrap();

        // check that there is an mcommit sent to everyone
        assert!(mcommit_tosend.to_processes());
        if let ToSend::ToProcesses(to, _) = mcommit_tosend.clone() {
            assert_eq!(to.len(), n);
        } else {
            panic!("ToSend::ToProcesses not found!");
        }

        // all processes handle it
        let mut nothings = router.route(mcommit_tosend, &time).into_iter();
        // the first one has a reply to a client, the remaining two are nothings
        let to_client = nothings.next().unwrap();
        assert!(to_client.to_clients());
        assert_eq!(nothings.next().unwrap(), ToSend::Nothing);
        assert_eq!(nothings.next().unwrap(), ToSend::Nothing);
        assert_eq!(nothings.next(), None);

        // handle what was sent to client
        let new_submit = router.route(to_client, &time).into_iter().next().unwrap();
        assert!(new_submit.to_coordinator());

        let mcollect = router.route(new_submit, &time).into_iter().next().unwrap();
        if let ToSend::ToProcesses(_, Message::MCollect { from, dot, .. }) = mcollect {
            assert_eq!(from, target);
            assert_eq!(dot, Dot::new(target, 2));
        } else {
            panic!("Message::MCollect not found!");
        }
    }
}
