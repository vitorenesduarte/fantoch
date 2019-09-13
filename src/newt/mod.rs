// This module contains the definition of `Clocks`.
mod clocks;

// This module contains the definition of `ProcVotes`, `Votes` and `VoteRange`.
mod votes;

// This module contains the definition of `MultiVotesTable`.
mod votes_table;

// This module contains the definition of `QuorumClocks`.
mod quorum_clocks;

// This module contains the definition of `Router`.
mod router;

use crate::base::{BaseProc, ClientId, Dot, ProcId, Rifl};
use crate::command::{Command, MultiCommand, MultiCommandResult, Pending};
use crate::config::Config;
use crate::newt::clocks::Clocks;
use crate::newt::quorum_clocks::QuorumClocks;
use crate::newt::votes::{ProcVotes, Votes};
use crate::newt::votes_table::MultiVotesTable;
use crate::planet::{Planet, Region};
use crate::store::KVStore;
use crate::store::Key;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

pub struct Newt {
    bp: BaseProc,
    clocks: Clocks,
    dot_to_info: HashMap<Dot, Info>,
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
        let dot_to_info = HashMap::new();
        let store = KVStore::new();
        let pending = Pending::new();

        // create `Newt`
        Newt {
            bp,
            clocks,
            dot_to_info,
            table,
            store,
            pending,
        }
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
        let dot = self.next_dot();

        // compute its clock
        let clock = self.clocks.clock(&cmd) + 1;

        // clone the fast quorum
        let fast_quorum = self.fast_quorum.clone().unwrap();

        // create `MCollect`
        let mcollect = Message::MCollect {
            from: self.id,
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
        // get message info
        let info = self.dot_to_info.entry(dot).or_insert_with(Info::new);

        // discard message if no longer in START
        if info.status != Status::START {
            return ToSend::Nothing;
        }

        // compute command clock
        let clock = std::cmp::max(clock, self.clocks.clock(&cmd) + 1);

        // compute proc votes
        let proc_votes = self.clocks.proc_votes(&cmd, clock);

        // bump all keys clocks to be `clock`
        self.clocks.bump_to(&cmd, clock);

        // TODO we could probably save HashMap operations if the previous two
        // steps are performed together

        // create votes and quorum clocks
        let votes = Votes::from(&cmd);
        let quorum_clocks = QuorumClocks::from(self.bp.q);

        // TODO above we have the same borrow checker problem that doesn't know
        // how to deref, as in the MCollectAck handler

        // update info
        info.status = Status::COLLECT;
        info.quorum = quorum;
        info.cmd = Some(cmd);
        info.clock = clock;
        info.votes = votes;
        info.quorum_clocks = quorum_clocks;

        // create `MCollectAck`
        let mcollectack = Message::MCollectAck {
            from: self.id,
            dot,
            clock,
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
        proc_votes: ProcVotes,
    ) -> ToSend {
        // get message info
        let info = self.dot_to_info.entry(dot).or_insert_with(Info::new);

        if info.status != Status::COLLECT || info.quorum_clocks.contains(&from)
        {
            // do nothing if we're no longer COLLECT or if this is a
            // duplicated message
            return ToSend::Nothing;
        }
        // update votes
        info.votes.add(proc_votes);

        // update quorum clocks
        info.quorum_clocks.add(from, clock);

        // TODO local clock bump upon each `MCollectAck`

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // compute max and its number of occurences
            let (max_clock, max_count) = info.quorum_clocks.max_and_count();

            // fast path condition: if the max was reported by at least f
            // processes
            if max_count >= self.bp.config.f() {
                // TODO above, we had to use self.bp.config because the
                // borrow-checker couldn't figure it out:
                // - is it some issue when dereferencing?

                // create `MCommit`
                let mcommit = Message::MCommit {
                    dot,
                    cmd: info.cmd.clone().unwrap(),
                    clock: max_clock,
                    votes: info.votes.clone(),
                };

                // return `ToSend`
                ToSend::Procs(mcommit, self.all_procs.clone().unwrap())
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
        cmd: MultiCommand,
        clock: u64,
        votes: Votes,
    ) -> ToSend {
        // get original proc id
        let proc_id = dot.0;

        // get message info
        let info = self.dot_to_info.entry(dot).or_insert_with(Info::new);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return ToSend::Nothing;
        }

        // update info
        info.status = Status::COMMIT;
        info.cmd = Some(cmd);
        info.clock = clock;
        info.votes = votes;

        // TODO generate phantom votes if committed clock is higher than the
        // local key's clock

        // update votes table and get commands that can be executed
        let to_execute = self.table.add(
            proc_id,
            info.cmd.clone(),
            info.clock,
            info.votes.clone(),
        );

        // execute commands
        if let Some(to_execute) = to_execute {
            let ready_commands = self.execute(to_execute);
            // if there ready commands, forward them to clients
            if ready_commands.is_empty() {
                ToSend::Nothing
            } else {
                ToSend::Clients(ready_commands)
            }
        } else {
            // no message to be sent
            ToSend::Nothing
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
                let res = self.pending.add(cmd_id, key.clone(), cmd_result);

                // if there's a new `MultiCommand` ready, add it to output var
                if let Some(ready) = res {
                    let rifl = ready.id();
                    let client_id = rifl.0;
                    let client_ready = ready_commands
                        .entry(client_id)
                        .or_insert_with(Vec::new);
                    client_ready.push((rifl, ready));
                }
            }
        }

        ready_commands
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
        cmd: MultiCommand,
        clock: u64,
        votes: Votes,
    },
}

// `Info` contains all information required in the life-cyle of a `Command`
struct Info {
    status: Status,
    quorum: Vec<ProcId>,
    cmd: Option<MultiCommand>, // `None` if noOp
    clock: u64,
    votes: Votes,
    quorum_clocks: QuorumClocks,
}

impl Info {
    fn new() -> Self {
        Info {
            status: Status::START,
            quorum: vec![],
            cmd: None,
            clock: 0,
            votes: Votes::uninit(),
            quorum_clocks: QuorumClocks::uninit(),
        }
    }
}

/// `Status` of commands.
#[derive(PartialEq)]
enum Status {
    START,
    COLLECT,
    COMMIT,
}

// with `Deref` and `DerefMut`, we can use `BaseProc` variables and methods
// as if they were defined in `Newt`, much like `Newt` inherits from
// `BaseProc`
impl Deref for Newt {
    type Target = BaseProc;

    fn deref(&self) -> &Self::Target {
        &self.bp
    }
}

impl DerefMut for Newt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bp
    }
}

#[cfg(test)]
mod tests {
    use crate::base::Client;
    use crate::command::MultiCommand;
    use crate::config::Config;
    use crate::newt::router::Router;
    use crate::newt::{Message, Newt, ToSend};
    use crate::planet::{Planet, Region};

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
        // procs
        let procs = vec![
            (0, Region::new("europe-west2")),
            (1, Region::new("europe-west3")),
            (2, Region::new("europe-west4")),
        ];

        // planet
        let planet = Planet::new("latency/");

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // newts
        let mut newt_0 = Newt::new(
            0,
            Region::new("europe-west2"),
            planet.clone(),
            config.clone(),
        );
        let mut newt_1 = Newt::new(
            1,
            Region::new("europe-west3"),
            planet.clone(),
            config.clone(),
        );
        let mut newt_2 = Newt::new(
            2,
            Region::new("europe-west4"),
            planet.clone(),
            config.clone(),
        );

        // discover procs in all newts
        newt_0.discover(procs.clone());
        newt_1.discover(procs.clone());
        newt_2.discover(procs.clone());

        // create msg router
        let mut router = Router::new();

        // register processes
        router.set_proc(0, newt_0);
        router.set_proc(1, newt_1);
        router.set_proc(2, newt_2);

        // create client 100 that is connected to newt 0
        let mut client_100 = Client::new(100, 0);
        // start client 100
        let (proc_0, cmd) = client_100.start();

        // register clients
        router.set_client(100, client_100);

        // submit it in newt_0
        let msubmit = Message::Submit { cmd };
        let mcollects = router.route_to_proc(proc_0, msubmit);

        // check that the mcollect is being sent to 2 processes
        assert!(mcollects.to_procs());
        if let ToSend::Procs(_, to) = mcollects.clone() {
            assert_eq!(to.len(), 2 * f);
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
        let new_submit = router.route(to_client);
    }
}
