use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{Dot, ProcessId, Rifl};
use crate::kvs::KVStore;
use crate::log;
use crate::planet::{Planet, Region};
use crate::protocol::common::dependency::{DependencyGraph, KeysClocks, QuorumClocks};
use crate::protocol::common::{Synod, SynodMessage};
use crate::protocol::{BaseProcess, Process, ToSend};
use crate::util;
use std::collections::{HashMap, HashSet};
use std::mem;
use threshold::VClock;

pub struct Atlas {
    bp: BaseProcess,
    keys_clocks: KeysClocks,
    cmds_info: CommandsInfo,
    graph: DependencyGraph,
    store: KVStore,
    pending: HashSet<Rifl>,
    commands_ready: Vec<CommandResult>,
}

impl Process for Atlas {
    type Message = Message;

    /// Creates a new `Atlas` process.
    fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) = Atlas::quorum_sizes(&config);

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            region,
            planet,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let keys_clocks = KeysClocks::new(config.n());
        let cmds_info = CommandsInfo::new(process_id, config.n(), config.f(), fast_quorum_size);
        let graph = DependencyGraph::new(&config);
        let store = KVStore::new();
        let pending = HashSet::new();
        let commands_ready = Vec::new();

        // create `Atlas`
        Self {
            bp,
            keys_clocks,
            cmds_info,
            graph,
            store,
            pending,
            commands_ready,
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
    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> ToSend<Self::Message> {
        match msg {
            Message::MCollect {
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock),
            Message::MCollectAck { dot, clock } => self.handle_mcollectack(from, dot, clock),
            Message::MCommit { dot, value } => self.handle_mcommit(from, dot, value),
            Message::MConsensus { dot, ballot, value } => {
                self.handle_mconsensus(from, dot, ballot, value)
            }
            Message::MConsensusAck { dot, ballot } => self.handle_mconsensusack(from, dot, ballot),
        }
    }

    /// Returns new commands results to be sent to clients.
    fn commands_ready(&mut self) -> Vec<CommandResult> {
        let mut ready = Vec::new();
        mem::swap(&mut ready, &mut self.commands_ready);
        ready
    }

    fn show_stats(&self) {
        self.graph.show_stats();
    }
}

impl Atlas {
    /// Computes `Atlas` fast and write quorum sizes.
    fn quorum_sizes(config: &Config) -> (usize, usize) {
        let n = config.n();
        let f = config.f();
        let fast_quorum_size = (n / 2) + f;
        let write_quorum_size = f + 1;
        (fast_quorum_size, write_quorum_size)
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: Command) -> ToSend<Message> {
        // start command in `Pending`
        assert!(self.pending.insert(cmd.rifl()));

        // compute the command identifier
        let dot = self.bp.next_dot();

        // wrap command
        let cmd = Some(cmd);

        // compute its clock
        let clock = self.keys_clocks.clock(&cmd);
        self.keys_clocks.add(dot, &cmd);

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        ToSend::ToProcesses(self.id(), target, mcollect)
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Option<Command>,
        quorum: Vec<ProcessId>,
        remote_clock: VClock<ProcessId>,
    ) -> ToSend<Message> {
        log!(
            "p{}: MCollect({:?}, {:?}, {:?}) from {}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from
        );

        // get cmd info
        let info = self.cmds_info.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return ToSend::Nothing;
        }

        // compute its clock
        let clock = self.keys_clocks.clock_with_past(&cmd, remote_clock);
        self.keys_clocks.add(dot, &cmd);

        // update command info
        info.status = Status::COLLECT;
        info.quorum = quorum;
        // create and set consensus value
        let value = ConsensusValue::with(cmd, clock.clone());
        assert!(info.synod.maybe_set_value(|| value));

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck { dot, clock };
        let target = vec![from];

        // return `ToSend`
        ToSend::ToProcesses(self.id(), target, mcollectack)
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: VClock<ProcessId>,
    ) -> ToSend<Message> {
        log!(
            "p{}: MCollectAck({:?}, {:?}) from {}",
            self.id(),
            dot,
            clock,
            from
        );

        // get cmd info
        let info = self.cmds_info.get(dot);

        if info.status != Status::COLLECT {
            // do nothing if we're no longer COLLECT
            return ToSend::Nothing;
        }

        // update quorum clocks
        info.quorum_clocks.add(from, clock);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // compute the threshold union while checking whether it's equal to their union
            let (final_clock, equal_to_union) =
                info.quorum_clocks.threshold_union(self.bp.config.f());

            // create consensus value
            // TODO can the following be more performant or at least more ergonomic?
            let cmd = info.synod.value().clone().cmd;
            let value = ConsensusValue::with(cmd, final_clock);

            // fast path condition:
            // - each dependency was reported by at least f processes
            if equal_to_union {
                // fast path: create `MCommit`
                // TODO create a slim-MCommit that only sends the payload to the non-fast-quorum
                // members, or send the payload to all in a slim-MConsensus
                let mcommit = Message::MCommit { dot, value };
                let target = self.bp.all();

                // return `ToSend`
                ToSend::ToProcesses(self.id(), target, mcommit)
            } else {
                // slow path: create `MConsensus`
                let ballot = info.synod.first_ballot();
                let mconsensus = Message::MConsensus { dot, ballot, value };
                let target = self.bp.write_quorum();
                // return `ToSend`
                ToSend::ToProcesses(self.id(), target, mconsensus)
            }
        } else {
            ToSend::Nothing
        }
    }

    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        value: ConsensusValue,
    ) -> ToSend<Message> {
        log!("p{}: MCommit({:?}, {:?})", self.id(), dot, value.clock);

        // get cmd info
        let info = self.cmds_info.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return ToSend::Nothing;
        }

        // update command info:
        info.status = Status::COMMIT;
        info.synod
            .handle(from, SynodMessage::MChosen(value.clone()));

        // add to graph if not a noop and execute commands that can be executed
        if let Some(cmd) = value.cmd {
            self.graph.add(dot, cmd, value.clock);
            let to_execute = self.graph.commands_to_execute();
            self.execute(to_execute);
        }

        // nothing to send
        ToSend::Nothing
    }

    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
    ) -> ToSend<Message> {
        log!(
            "p{}: MConsensus({:?}, {}, {:?})",
            self.id(),
            dot,
            ballot,
            value.clock
        );

        // get cmd info
        let info = self.cmds_info.get(dot);

        // compute message: that can either be nothing, an ack or an mcommit
        let msg = match info
            .synod
            .handle(from, SynodMessage::MAccept(ballot, value))
        {
            Some(SynodMessage::MAccepted(ballot)) => {
                // the accept message was accepted:
                // create `MConsensusAck`
                Message::MConsensusAck { dot, ballot }
            }
            Some(SynodMessage::MChosen(value)) => {
                // the value has already been chosen:
                // create `MCommit`
                Message::MCommit { dot, value }
            }
            None => {
                // ballot too low to be accepted
                return ToSend::Nothing;
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensus handler"
            ),
        };

        // create target
        let target = vec![from];

        // return `ToSend`
        ToSend::ToProcesses(self.id(), target, msg)
    }

    fn handle_mconsensusack(&mut self, from: ProcessId, dot: Dot, ballot: u64) -> ToSend<Message> {
        log!("p{}: MConsensusAck({:?}, {})", self.id(), dot, ballot);

        // get cmd info
        let info = self.cmds_info.get(dot);

        // compute message: that can either be nothing or an mcommit
        match info.synod.handle(from, SynodMessage::MAccepted(ballot)) {
            Some(SynodMessage::MChosen(value)) => {
                // enough accepts were gathered and the value has been chosen
                // create `MCommit` and target
                // create target
                let target = self.bp.all();
                let mcommit = Message::MCommit { dot, value };

                // return `ToSend`
                ToSend::ToProcesses(self.id(), target, mcommit)
            }
            None => {
                // not enough accepts yet
                ToSend::Nothing
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensusAck handler"
            ),
        }
    }

    fn execute(&mut self, to_execute: Vec<Command>) {
        // borrow everything we'll need
        let commands_ready = &mut self.commands_ready;
        let store = &mut self.store;
        let pending = &mut self.pending;

        // get more commands that are ready to be executed
        let ready = to_execute.into_iter().filter_map(|cmd| {
            // get command rifl
            let rifl = cmd.rifl();
            // execute the command
            let result = store.execute_command(cmd);

            // if it was pending locally, then it's from a client of this process
            if pending.remove(&rifl) {
                Some(result)
            } else {
                None
            }
        });
        commands_ready.extend(ready);
    }
}

// `CommandsInfo` contains `CommandInfo` for each `Dot`.
struct CommandsInfo {
    process_id: ProcessId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    dot_to_info: HashMap<Dot, CommandInfo>,
}

impl CommandsInfo {
    fn new(process_id: ProcessId, n: usize, f: usize, fast_quorum_size: usize) -> Self {
        Self {
            process_id,
            n,
            f,
            fast_quorum_size,
            dot_to_info: HashMap::new(),
        }
    }

    // Returns the `CommandInfo` associated with `Dot`.
    // If no `CommandInfo` is associated, an empty `CommandInfo` is returned.
    fn get(&mut self, dot: Dot) -> &mut CommandInfo {
        // TODO borrow everything we need so that the borrow checker does not complain
        let process_id = self.process_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        self.dot_to_info
            .entry(dot)
            .or_insert_with(|| CommandInfo::new(process_id, n, f, fast_quorum_size))
    }
}

// consensus value is a pair where the first component is the command (noop if `None`) and the
// second component its dependencies represented as a vector clock.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsensusValue {
    cmd: Option<Command>,
    clock: VClock<ProcessId>,
}

impl ConsensusValue {
    fn new(n: usize) -> Self {
        let cmd = None;
        let clock = VClock::with(util::process_ids(n));
        Self { cmd, clock }
    }

    fn with(cmd: Option<Command>, clock: VClock<ProcessId>) -> Self {
        Self { cmd, clock }
    }
}

fn proposal_gen(_values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
    todo!("recovery not implemented yet")
}

// `CommandInfo` contains all information required in the life-cyle of a
// `Command`
struct CommandInfo {
    status: Status,
    quorum: Vec<ProcessId>,
    synod: Synod<ConsensusValue>,
    // `quorum_clocks` is used by the coordinator to compute the threshold clock when deciding
    // whether to take the fast path
    quorum_clocks: QuorumClocks,
}

impl CommandInfo {
    fn new(process_id: ProcessId, n: usize, f: usize, fast_quorum_size: usize) -> Self {
        // create bottom consensus value
        let initial_value = ConsensusValue::new(n);
        Self {
            status: Status::START,
            quorum: vec![],
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            quorum_clocks: QuorumClocks::new(fast_quorum_size),
        }
    }
}

// `Atlas` protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    MCollect {
        dot: Dot,
        cmd: Option<Command>, // it's never a noop though
        clock: VClock<ProcessId>,
        quorum: Vec<ProcessId>,
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
    use crate::sim::Simulation;
    use crate::time::SimTime;

    #[test]
    fn atlas_parameters() {
        let config = Config::new(7, 1);
        assert_eq!(Atlas::quorum_sizes(&config), (4, 2));

        let config = Config::new(7, 2);
        assert_eq!(Atlas::quorum_sizes(&config), (5, 3));

        let config = Config::new(7, 3);
        assert_eq!(Atlas::quorum_sizes(&config), (6, 4));
    }

    #[test]
    fn atlas_flow() {
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
        let planet = Planet::new("latency/");

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // atlas
        let mut atlas_1 = Atlas::new(process_id_1, europe_west2.clone(), planet.clone(), config);
        let mut atlas_2 = Atlas::new(process_id_2, europe_west3.clone(), planet.clone(), config);
        let mut atlas_3 = Atlas::new(process_id_3, us_west1.clone(), planet.clone(), config);

        // discover processes in all atlas
        atlas_1.discover(processes.clone());
        atlas_2.discover(processes.clone());
        atlas_3.discover(processes.clone());

        // create simulation
        let mut simulation = Simulation::new();

        // register processes
        simulation.register_process(atlas_1);
        simulation.register_process(atlas_2);
        simulation.register_process(atlas_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // create client 1 that is connected to atlas 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, client_region, planet.clone(), workload);

        // discover processes in client 1
        assert!(client_1.discover(processes));

        // start client
        let (target, cmd) = client_1.start(&time);

        // check that `target` is atlas 1
        assert_eq!(target, process_id_1);

        // register clients
        simulation.register_client(client_1);

        // submit it in atlas_0
        let mcollects = simulation.get_process(target).submit(cmd);

        // check that the mcollect is being sent to 2 processes
        assert!(mcollects.to_processes());
        if let ToSend::ToProcesses(_, to, _) = mcollects.clone() {
            assert_eq!(to.len(), 2 * f);
            assert_eq!(to, vec![1, 2]);
        } else {
            panic!("ToSend::ToProcesses not found!");
        }

        // handle in mcollects
        let mut mcollectacks = simulation.forward_to_processes(mcollects);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);
        assert!(mcollectacks.iter().all(|to_send| to_send.to_processes()));

        // handle the first mcollectack
        let mut mcommits = simulation.forward_to_processes(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();
        // no mcommit yet
        assert!(mcommit_tosend.is_nothing());

        // handle the second mcollectack
        let mut mcommits = simulation.forward_to_processes(mcollectacks.pop().unwrap());
        let mcommit_tosend = mcommits.pop().unwrap();

        // check that there is an mcommit sent to everyone
        assert!(mcommit_tosend.to_processes());
        if let ToSend::ToProcesses(_, to, _) = mcommit_tosend.clone() {
            assert_eq!(to.len(), n);
        } else {
            panic!("ToSend::ToProcesses not found!");
        }

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mcommit_tosend);

        // there's nothing to send
        let not_nothing_count = to_sends
            .into_iter()
            .filter(|to_send| !to_send.is_nothing())
            .count();
        assert_eq!(not_nothing_count, 0);

        // process 1 should have a result to the client
        let commands_ready = simulation.get_process(process_id_1).commands_ready();
        assert_eq!(commands_ready.len(), 1);

        // handle what was sent to client
        let new_submit = simulation
            .forward_to_clients(commands_ready, &time)
            .into_iter()
            .next()
            .unwrap();
        assert!(new_submit.to_coordinator());

        let mcollect = simulation
            .forward_to_processes(new_submit)
            .into_iter()
            .next()
            .unwrap();
        if let ToSend::ToProcesses(from, _, Message::MCollect { dot, .. }) = mcollect {
            assert_eq!(from, target);
            assert_eq!(dot, Dot::new(target, 2));
        } else {
            panic!("Message::MCollect not found!");
        }
    }
}
