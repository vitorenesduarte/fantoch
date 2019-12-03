// This module contains the definition of `Clocks`.
mod clocks;

// This module contains the definition of `ProcVotes`, `Votes` and `VoteRange`.
mod votes;

// This module contains the definition of `MultiVotesTable`.
mod votes_table;

// This module contains the definition of `QuorumClocks`.
mod quorum_clocks;

use crate::base::{BaseProc, Dot, ProcId};
use crate::client::{ClientId, Rifl};
use crate::config::Config;
use crate::kvs::command::{Command, MultiCommand, MultiCommandResult};
use crate::kvs::pending::Pending;
use crate::kvs::store::{KVStore, Key};
use crate::newt::clocks::Clocks;
use crate::newt::quorum_clocks::QuorumClocks;
use crate::newt::votes::{ProcVotes, Votes};
use crate::newt::votes_table::MultiVotesTable;
use crate::planet::{Planet, Region};
use std::collections::HashMap;

pub struct Newt {
    bp: BaseProc,
    clocks: Clocks,
    cmds_info: CommandsInfo,
    table: MultiVotesTable,
    store: KVStore,
    pending: Pending,
}

impl Newt {
    /// Creates a new `Newt` proc.
    pub fn new(
        id: ProcId,
        region: Region,
        planet: Planet,
        config: Config,
    ) -> Self {
        // compute fast quorum size and stability threshold
        let q = Newt::fast_quorum_size(&config);
        let stability_threshold = Newt::stability_threshold(&config);

        // create `MultiVotesTable`
        let table = MultiVotesTable::new(config.n(), stability_threshold);

        // create `BaseProc`, `Clocks`, dot_to_info, `KVStore` and `Pending`.
        let bp = BaseProc::new(id, region, planet, config, q);
        let clocks = Clocks::new(id);
        let cmds_info = CommandsInfo::new(q);
        let store = KVStore::new();
        let pending = Pending::new();

        // create `Newt`
        Self {
            bp,
            clocks,
            cmds_info,
            table,
            store,
            pending,
        }
    }

    /// Returns the process identifier.
    pub fn id(&self) -> ProcId {
        self.bp.id
    }

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

    /// Handles messages by forwarding them to the respective handler.
    pub fn handle(&mut self, msg: Message) -> ToSend {
        match msg {
            Message::Submit { cmd } => self.handle_submit(cmd),
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
                proc_votes,
            } => self.handle_mcollectack(from, dot, clock, proc_votes),
            Message::MCommit {
                dot,
                cmd,
                clock,
                votes,
            } => self.handle_mcommit(dot, cmd, clock, votes),
        }
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: MultiCommand) -> ToSend {
        // start command in `Pending`
        self.pending.start(&cmd);

        // compute the command identifier
        let dot = self.bp.next_dot();

        // compute its clock
        let clock = self.clocks.clock(&cmd) + 1;

        // clone the fast quorum
        let fast_quorum = self.bp.fast_quorum.clone().unwrap();

        // create `MCollect`
        let mcollect = Message::MCollect {
            from: self.bp.id,
            dot,
            cmd,
            clock,
            quorum: fast_quorum.clone(),
        };

        // return `ToSend`
        ToSend::Procs(mcollect, fast_quorum)
    }

    fn handle_mcollect(
        &mut self,
        from: ProcId,
        dot: Dot,
        cmd: MultiCommand,
        quorum: Vec<ProcId>,
        clock: u64,
    ) -> ToSend {
        // get cmd info
        let info = self.cmds_info.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return ToSend::Nothing;
        }

        // TODO can we somehow combine a subset of the next 3 operations in
        // order to save HashMap operations?

        // compute command clock
        let cmd_clock = std::cmp::max(clock, self.clocks.clock(&cmd) + 1);

        // compute votes consumed by this command
        // - this computation needs to occur before the next `bump_to`
        let proc_votes = self.clocks.proc_votes(&cmd, cmd_clock);

        // if coordinator, set keys in `info.votes`
        // (`info.quorum_clocks` is initialized in `self.cmds_info.get`)
        let original_coordinator = dot.0;
        if self.bp.id == original_coordinator {
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
            from: self.bp.id,
            dot,
            clock: info.clock,
            proc_votes,
        };

        // return `ToSend`
        ToSend::Procs(mcollectack, vec![from])
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcId,
        dot: Dot,
        clock: u64,
        remote_proc_votes: ProcVotes,
    ) -> ToSend {
        // get cmd info
        let info = self.cmds_info.get(dot);

        if info.status != Status::COLLECT || info.quorum_clocks.contains(from) {
            // do nothing if we're no longer COLLECT or if this is a
            // duplicated message
            return ToSend::Nothing;
        }

        // update votes with remove votes
        info.votes.add(remote_proc_votes);

        // update quorum clocks while computing max clock and its number of
        // occurences
        let (max_clock, max_count) = info.quorum_clocks.add(from, clock);

        // optimization: bump all keys clocks in `cmd` to be `max_clock`
        // - this prevents us from generating votes (either clients submit new
        //   operations or when handling `MCollect` from other processes) that
        //   could potentially delay the execution of this command
        let cmd = info.cmd.as_ref().unwrap();
        let local_proc_votes = self.clocks.proc_votes(cmd, max_clock);
        info.votes.add(local_proc_votes);

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
                ToSend::Procs(mcommit, self.bp.all_procs.clone().unwrap())
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
        cmd: Option<MultiCommand>,
        clock: u64,
        votes: Votes,
    ) -> ToSend {
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
        let original_coordinator = dot.0;
        let to_execute = self.table.add(
            original_coordinator,
            info.cmd.clone(),
            info.clock,
            votes,
        );

        // execute commands
        match to_execute {
            Some(to_execute) => {
                let ready_commands = self.execute(to_execute);
                // if there ready commands, forward them to clients
                if ready_commands.is_empty() {
                    ToSend::Nothing
                } else {
                    ToSend::Clients(ready_commands)
                }
            }
            // no message to be sent
            None => ToSend::Nothing,
        }
    }

    fn execute(
        &mut self,
        to_execute: HashMap<Key, Vec<(Rifl, Command)>>,
    ) -> HashMap<ClientId, Vec<(Rifl, MultiCommandResult)>> {
        // TODO I couldn't do the following with iterators. Try again.
        // create variable that will hold all ready commads
        let mut ready_commands = HashMap::new();

        // iterate all commands to be executed
        for (key, cmds) in to_execute {
            for (cmd_id, cmd_action) in cmds {
                // execute cmd in the `KVStore`
                let cmd_result = self.store.execute(&key, cmd_action);

                // add partial result to `Pending`
                let res =
                    self.pending.add_partial(cmd_id, key.clone(), cmd_result);

                // if there's a new `MultiCommand` ready, add it to output var
                if let Some(ready) = res {
                    // get rifl and client id
                    let rifl = ready.rifl();
                    let client_id = rifl.client_id();

                    // get the commands already ready for this client and add a
                    // new one
                    ready_commands
                        .entry(client_id)
                        .or_insert_with(Vec::new)
                        .push((rifl, ready));
                }
            }
        }

        ready_commands
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
    quorum: Vec<ProcId>,
    cmd: Option<MultiCommand>, // `None` if noOp
    clock: u64,
    // `votes` is used by the coordinator to aggregate `ProcVotes` from fast
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

// every handle returns a `ToSend`
#[derive(Debug, Clone, PartialEq)]
pub enum ToSend {
    // nothing to send
    Nothing,
    // a protocol message to be sent to some processes
    Procs(Message, Vec<ProcId>),
    // a list of command results to be sent to the issuing client
    Clients(HashMap<ClientId, Vec<(Rifl, MultiCommandResult)>>),
}

#[allow(dead_code)] // TODO remove me
impl ToSend {
    /// Check if there's nothing to be sent.
    fn nothing(&self) -> bool {
        *self == ToSend::Nothing
    }

    /// Check if there's something to be sent to processes.
    fn to_procs(&self) -> bool {
        match *self {
            ToSend::Procs(_, _) => true,
            _ => false,
        }
    }

    /// Check if there's something to be sent to clients.
    fn to_clients(&self) -> bool {
        match *self {
            ToSend::Clients(_) => true,
            _ => false,
        }
    }
}

// `Newt` protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Submit {
        cmd: MultiCommand,
    },
    MCollect {
        from: ProcId,
        dot: Dot,
        cmd: MultiCommand,
        quorum: Vec<ProcId>,
        clock: u64,
    },
    MCollectAck {
        from: ProcId,
        dot: Dot,
        clock: u64,
        proc_votes: ProcVotes,
    },
    MCommit {
        dot: Dot,
        cmd: Option<MultiCommand>,
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
    use crate::client::Client;
    use crate::router::Router;

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
        // procs ids
        let proc_id_1 = 1;
        let proc_id_2 = 2;
        let proc_id_3 = 3;

        // procs
        let procs = vec![
            (proc_id_1, Region::new("europe-west2")),
            (proc_id_2, Region::new("europe-west3")),
            (proc_id_3, Region::new("europe-west4")),
        ];

        // planet
        let planet = Planet::new("latency/");

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // newts
        let mut newt_1 = Newt::new(
            proc_id_1,
            Region::new("europe-west2"),
            planet.clone(),
            config.clone(),
        );
        let mut newt_2 = Newt::new(
            proc_id_2,
            Region::new("europe-west3"),
            planet.clone(),
            config.clone(),
        );
        let mut newt_3 = Newt::new(
            proc_id_3,
            Region::new("europe-west4"),
            planet.clone(),
            config.clone(),
        );

        // discover procs in all newts
        newt_1.bp.discover(procs.clone());
        newt_2.bp.discover(procs.clone());
        newt_3.bp.discover(procs.clone());

        // create msg router
        let mut router = Router::new();

        // register processes
        router.register_proc(newt_1);
        router.register_proc(newt_2);
        router.register_proc(newt_3);

        // create client 100 that is connected to newt 1
        let mut client_100 = Client::new(100, proc_id_1);
        // start client 100
        let (target_proc, cmd) = client_100.start();

        // check that `target_proc` is newt 1
        assert_eq!(target_proc, proc_id_1);

        // register clients
        router.register_client(client_100);

        // submit it in newt_0
        let msubmit = Message::Submit { cmd };
        let mcollects = router.route_to_proc(target_proc, msubmit);

        // check that the mcollect is being sent to 2 processes
        assert!(mcollects.to_procs());
        if let ToSend::Procs(_, to) = mcollects.clone() {
            assert_eq!(to.len(), 2 * f);
        } else {
            panic!("ToSend::Procs not found!");
        }

        // handle in mcollects
        let mut mcollectacks = router.route(mcollects);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);
        assert!(mcollectacks.iter().all(|to_send| to_send.to_procs()));

        // handle the first mcollectack
        let mut mcommits = router.route(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();
        // no mcommit yet
        assert!(mcommit_tosend.nothing());

        // handle the second mcollectack
        let mut mcommits = router.route(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();

        // check that there is an mcommit sent to everyone
        assert!(mcommit_tosend.to_procs());
        if let ToSend::Procs(_, to) = mcommit_tosend.clone() {
            assert_eq!(to.len(), n);
        } else {
            panic!("ToSend::Procs not found!");
        }

        // all processes handle it
        let mut nothings = router.route(mcommit_tosend).into_iter();
        // the first one has a reply to a client, the remaining two are nothings
        let to_client = nothings.next().unwrap();
        assert!(to_client.to_clients());
        assert_eq!(nothings.next().unwrap(), ToSend::Nothing);
        assert_eq!(nothings.next().unwrap(), ToSend::Nothing);
        assert_eq!(nothings.next(), None);

        // handle what was sent to client
        let new_submit = router.route(to_client).into_iter().next().unwrap();
        assert!(new_submit.to_procs());

        let mcollect = router.route(new_submit).into_iter().next().unwrap();
        if let ToSend::Procs(Message::MCollect { from, dot, .. }, _) = mcollect
        {
            assert_eq!(from, target_proc);
            assert_eq!(dot, (target_proc, 2));
        } else {
            panic!("Message::MCollect not found!");
        }
    }
}
