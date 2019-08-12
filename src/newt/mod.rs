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

use crate::base::{BaseProc, Dot, ProcId, Rifl};
use crate::command::{Command, MultiCommand};
use crate::config::Config;
use crate::newt::clocks::Clocks;
use crate::newt::quorum_clocks::QuorumClocks;
use crate::newt::votes::{ProcVotes, Votes};
use crate::newt::votes_table::MultiVotesTable;
use crate::planet::{Planet, Region};
use crate::store::Key;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

pub struct Newt {
    bp: BaseProc,
    clocks: Clocks,
    dot_to_info: HashMap<Dot, Info>,
    table: MultiVotesTable,
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

        // create `BaseProc`, `Clocks`, dot_to_info and `MultiVotesTable`
        let bp = BaseProc::new(id, region, planet, config, q);
        let clocks = Clocks::new(id);
        let dot_to_info = HashMap::new();
        let table = MultiVotesTable::new(stability_threshold);

        // create `Newt`
        Newt {
            bp,
            clocks,
            dot_to_info,
            table,
        }
    }

    /// Computes `Newt` fast quorum size.
    fn fast_quorum_size(config: &Config) -> usize {
        2 * config.f()
    }

    /// Computes `Newt` stability threshold.
    fn stability_threshold(config: &Config) -> usize {
        config.n() - config.f()
    }

    /// Handles messages by forwarding them to the respective handler.
    pub fn handle(&mut self, msg: Message) -> ToSend {
        match msg {
            Message::Submit { cmd } => self.handle_submit(cmd),
            Message::Execute { cmds } => self.handle_execute(cmds),
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
        Some((mcollect, fast_quorum))
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
            return None;
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
        // how toderef, as in the MCollectAck handler

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
        Some((mcollectack, vec![from]))
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
            return None;
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
                Some((mcommit, self.all_procs.clone().unwrap()))
            } else {
                // slow path
                None
            }
        } else {
            None
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
            return None;
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

        // if there's something to execute, send it to self
        to_execute.map(|cmds| (Message::Execute { cmds }, vec![self.id]))
    }

    fn handle_execute(&mut self, cmds: HashMap<Key, Vec<(Rifl, Command)>>) -> ToSend {
        None
    }
}

// every handle returns a `ToSend`
type ToSend = Option<(Message, Vec<ProcId>)>;

// `Newt` protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Submit {
        cmd: MultiCommand,
    },
    Execute {
        cmds: HashMap<Key, Vec<(Rifl, Command)>>,
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
    use crate::command::MultiCommand;
    use crate::config::Config;
    use crate::newt::router::Router;
    use crate::newt::{Message, Newt};
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
        router.set_proc(0, newt_0);
        router.set_proc(1, newt_1);
        router.set_proc(2, newt_2);

        // create a command
        let key_a = String::from("A");
        let cmd_id = (100, 1); // client 100, 1st op
        let cmd = MultiCommand::get(cmd_id, vec![key_a]);

        // submit it in newt_0
        let msubmit = Message::Submit { cmd };
        let mcollects = router.route(&0, msubmit);

        // check that the mcollect is being sent to 2 processes
        assert_eq!(mcollects.clone().unwrap().1.len(), 2 * f);

        // handle in mcollects
        let mut mcollectacks = router.route_to_many(mcollects);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the first mcollectack
        let mut mcommits = router.route_to_many(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();
        // no mcommit yet
        assert!(mcommit_tosend.is_none());

        // handle the second mcollectack
        let mut mcommits = router.route_to_many(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();
        // there's an mcommit now
        assert!(mcommit_tosend.is_some());

        // the mcommit is sent to everyone
        assert_eq!(mcommit_tosend.clone().unwrap().1.len(), n);

        // all processes handle it
        let nones = router.route_to_many(mcommit_tosend);
        // and no reply is sent
        assert_eq!(nones, vec![None, None, None]);
    }
}
