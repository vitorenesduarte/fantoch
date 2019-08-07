use crate::base::{BaseProc, Dot, ProcId};
use crate::clocks::Clocks;
use crate::command::Command;
use crate::config::Config;
use crate::planet::{Planet, Region};
use crate::votes::{ProcVotes, Votes};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

pub struct Newt {
    bp: BaseProc,
    clocks: Clocks,
    dot_to_info: HashMap<Dot, Info>,
}

impl Newt {
    /// Creates a new `Newt` proc.
    pub fn new(
        id: ProcId,
        region: Region,
        planet: Planet,
        config: Config,
    ) -> Self {
        // compute fast quorum size
        let fast_quorum_size = Newt::fast_quorum_size(&config);

        // create `BaseProc`
        let bp = BaseProc::new(id, region, planet, config, fast_quorum_size);

        // create `Newt`
        Newt {
            bp,
            clocks: Clocks::new(id),
            dot_to_info: HashMap::new(),
        }
    }

    /// Computes `Newt` fast quorum size.
    fn fast_quorum_size(config: &Config) -> usize {
        2 * config.f()
    }

    /// Handles messages by forwarding them to the respective handler.
    pub fn handle(&mut self, msg: Message) -> ToSend {
        match msg {
            Message::MSubmit { cmd } => self.handle_submit(cmd),
            Message::MCollect {
                from,
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock),
            Message::MCollectAck {
                dot,
                clock,
                proc_votes,
            } => self.handle_mcollectack(dot, clock, proc_votes),
            Message::MCommit {
                dot,
                cmd,
                clock,
                votes,
            } => self.handle_mcommit(dot, cmd, clock, votes),
        }
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: Command) -> ToSend {
        // compute the command identifier
        let dot = self.next_dot();

        // compute its clock
        let clock = self.clocks.clock(&cmd) + 1;

        // clone the fast quorum
        let fast_quorum = self.fast_quorum.as_ref().unwrap().clone();

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
        cmd: Command,
        quorum: Vec<ProcId>,
        clock: usize,
    ) -> ToSend {
        if self.dot_to_info.contains_key(&dot) {
            return None;
        }

        // compute command clock
        let clock = std::cmp::max(clock, self.clocks.clock(&cmd) + 1);

        // compute proc votes
        let proc_votes = self.clocks.proc_votes(&cmd, clock);

        // bump all objects clocks to be `clock`
        self.clocks.bump_to(&cmd, clock);

        // TODO we could probably save HashMap operations if the previous two
        // steps are performed together

        // create votes
        let votes = Votes::new(&cmd);

        // update dot to info
        let info = Info {
            status: Status::COLLECT,
            quorum,
            cmd: Some(cmd),
            clock,
            votes,
        };
        self.dot_to_info.insert(dot, info);

        // create `MCollectAck`
        let mcollectack = Message::MCollectAck {
            dot,
            clock,
            proc_votes,
        };

        // return `ToSend`
        Some((mcollectack, vec![from]))
    }

    fn handle_mcollectack(
        &mut self,
        dot: Dot,
        clock: usize,
        proc_votes: ProcVotes,
    ) -> ToSend {
        if let Some(info) = self.dot_to_info.get_mut(&dot) {
            if info.status == Status::COLLECT {
                None
            } else {
                None
            }
        } else {
            None
        }
    }

    fn handle_mcommit(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: usize,
        votes: Votes,
    ) -> ToSend {
        None
    }
}

// every handle returns a `ToSend`
type ToSend = Option<(Message, Vec<ProcId>)>;

// `Newt` protocol messages
#[derive(Debug, Clone)]
pub enum Message {
    MSubmit {
        cmd: Command,
    },
    MCollect {
        from: ProcId,
        dot: Dot,
        cmd: Command,
        quorum: Vec<ProcId>,
        clock: usize,
    },
    MCollectAck {
        dot: Dot,
        clock: usize,
        proc_votes: ProcVotes,
    },
    MCommit {
        dot: Dot,
        cmd: Command,
        clock: usize,
        votes: Votes,
    },
}

// `Info` contains all information required in the life-cyle of a `Command`
struct Info {
    status: Status,
    quorum: Vec<ProcId>,
    cmd: Option<Command>,
    clock: usize,
    votes: Votes,
}

/// `Status` of commands.
#[derive(PartialEq)]
enum Status {
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
    use crate::command::{Command, Object};
    use crate::config::Config;
    use crate::newt::{Message, Newt, ToSend};
    use crate::planet::{Planet, Region};
    use std::collections::HashMap;

    #[test]
    fn fast_quorum_size() {
        // n and f
        let n = 5;
        let f = 1;
        let config = Config::new(n, f);
        assert_eq!(Newt::fast_quorum_size(&config), 2);
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

        // create a command
        let cmd = Command::new(vec![Object::new("A")]);

        // submit it in newt_0
        let msubmit = Message::MSubmit { cmd };
        let mcollects = newt_0.handle(msubmit);

        // check that the mcollect is being sent to 2 processes
        assert_eq!(mcollects.clone().unwrap().1.len(), 2 * f);

        // handle in mcollects
        let mut mcollectacks =
            handle_in_target(mcollects, &mut newt_0, &mut newt_1, &mut newt_2);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the first mcollectack
        let mut mcommits = handle_in_target(
            mcollectacks.pop().unwrap(),
            &mut newt_0,
            &mut newt_1,
            &mut newt_2,
        );
        assert!(mcommits.pop().unwrap().is_none());

        // handle the second mcollectack
        let mut mcommits = handle_in_target(
            mcollectacks.pop().unwrap(),
            &mut newt_0,
            &mut newt_1,
            &mut newt_2,
        );
        assert!(mcommits.pop().unwrap().is_some());
    }

    fn handle_in_target(
        to_send: ToSend,
        newt_0: &mut Newt,
        newt_1: &mut Newt,
        newt_2: &mut Newt,
    ) -> Vec<ToSend> {
        if let Some((msg, target)) = to_send {
            target
                .into_iter()
                .map(|proc_id| match proc_id {
                    0 => newt_0.handle(msg.clone()),
                    1 => newt_1.handle(msg.clone()),
                    2 => newt_2.handle(msg.clone()),
                    _ => {
                        panic!("unhandled process id");
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    }
}
