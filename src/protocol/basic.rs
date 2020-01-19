use crate::command::Command;
use crate::config::Config;
use crate::executor::{BasicExecutor, Executor};
use crate::id::{Dot, ProcessId};
use crate::protocol::common::info::{Commands, Info};
use crate::protocol::{BaseProcess, Protocol, ToSend};
use crate::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;

type ExecutionInfo = <BasicExecutor as Executor>::ExecutionInfo;

pub struct Basic {
    bp: BaseProcess,
    cmds: Commands<CommandInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl Protocol for Basic {
    type Message = Message;
    type Executor = BasicExecutor;

    /// Creates a new `Basic` process.
    fn new(process_id: ProcessId, config: Config) -> Self {
        // compute fast and write quorum sizes
        let fast_quorum_size = config.basic_quorum_size();
        let write_quorum_size = 0; // there's no write quorum as we have 100% fast paths

        // create protocol data-structures
        let bp = BaseProcess::new(process_id, config, fast_quorum_size, write_quorum_size);
        let cmds = Commands::new(process_id, config.n(), config.f(), fast_quorum_size);
        let to_executor = Vec::new();

        // create `Basic`
        Self {
            bp,
            cmds,
            to_executor,
        }
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
    fn submit(&mut self, cmd: Command) -> ToSend<Message> {
        self.handle_submit(cmd)
    }

    /// Handles protocol messages.
    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> Option<ToSend<Message>> {
        match msg {
            Message::MStore { dot, cmd } => self.handle_mstore(from, dot, cmd),
            Message::MStoreAck { dot } => self.handle_mstoreack(from, dot),
            Message::MCommit { dot, cmd } => self.handle_mcommit(from, dot, cmd),
        }
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        mem::take(&mut self.to_executor)
    }

    fn show_metrics(&self) {
        self.bp.show_metrics();
    }
}

impl Basic {
    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: Command) -> ToSend<Message> {
        // compute the command identifier
        let dot = self.bp.next_dot();

        // create `MStore` and target
        let mstore = Message::MStore { dot, cmd };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        ToSend {
            from: self.id(),
            target,
            msg: mstore,
        }
    }

    fn handle_mstore(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MStore({:?}, {:?}) from {}", self.id(), dot, cmd, from);

        // get cmd info
        let info = self.cmds.get(dot);

        // update command info
        info.cmd = Some(cmd);

        // create `MStoreAck` and target
        let mstoreack = Message::MStoreAck { dot };
        let target = singleton![from];

        // return `ToSend`
        Some(ToSend {
            from: self.id(),
            target,
            msg: mstoreack,
        })
    }

    fn handle_mstoreack(&mut self, from: ProcessId, dot: Dot) -> Option<ToSend<Message>> {
        log!("p{}: MStoreAck({:?}) from {}", self.id(), dot, from);

        // get cmd info
        let info = self.cmds.get(dot);

        // update quorum clocks
        info.missing_acks -= 1;

        // check if we have all necessary replies
        if info.missing_acks == 0 {
            let mcommit = Message::MCommit {
                dot,
                cmd: info.cmd.clone().expect("command should exist"),
            };
            let target = self.bp.all();

            // return `ToSend`
            Some(ToSend {
                from: self.id(),
                target,
                msg: mcommit,
            })
        } else {
            None
        }
    }

    fn handle_mcommit(
        &mut self,
        _from: ProcessId,
        dot: Dot,
        cmd: Command,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MCommit({:?}, {:?})", self.id(), dot, cmd);

        // create execution info
        self.to_executor.push(cmd);

        // TODO the following is incorrect: it should only be deleted once it has been committed at
        // all processes
        self.cmds.remove(dot);

        // nothing to send
        None
    }
}

// `CommandInfo` contains all information required in the life-cyle of a `Command`
struct CommandInfo {
    cmd: Option<Command>,
    missing_acks: usize,
}

impl Info for CommandInfo {
    fn new(_process_id: ProcessId, _n: usize, _f: usize, fast_quorum_size: usize) -> Self {
        // create bottom consensus value
        Self {
            cmd: None,
            missing_acks: fast_quorum_size,
        }
    }
}

// `Basic` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    MStore { dot: Dot, cmd: Command },
    MStoreAck { dot: Dot },
    MCommit { dot: Dot, cmd: Command },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, Workload};
    use crate::planet::{Planet, Region};
    use crate::sim::Simulation;
    use crate::time::SimTime;
    use crate::util;

    #[test]
    fn basic_flow() {
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
        let planet = Planet::new("latency/");

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // executors
        let executor_1 = BasicExecutor::new(config);
        let executor_2 = BasicExecutor::new(config);
        let executor_3 = BasicExecutor::new(config);

        // basic
        let mut basic_1 = Basic::new(process_id_1, config);
        let mut basic_2 = Basic::new(process_id_2, config);
        let mut basic_3 = Basic::new(process_id_3, config);

        // discover processes in all basic
        let sorted = util::sort_processes_by_distance(&europe_west2, &planet, processes.clone());
        basic_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(&europe_west3, &planet, processes.clone());
        basic_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(&us_west1, &planet, processes.clone());
        basic_3.discover(sorted);

        // register processes
        simulation.register_process(basic_1, executor_1);
        simulation.register_process(basic_2, executor_2);
        simulation.register_process(basic_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // create client 1 that is connected to basic 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, workload);

        // discover processes in client 1
        let sorted = util::sort_processes_by_distance(&client_region, &planet, processes);
        assert!(client_1.discover(sorted));

        // start client
        let (target, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");

        // check that `target` is basic 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in basic 1
        let (process, executor) = simulation.get_process(target);
        executor.register(&cmd);
        let mcollect = process.submit(cmd);

        // check that the mcollect is being sent to 2 processes
        let ToSend { target, .. } = mcollect.clone();
        assert_eq!(target.len(), 2 * f);
        assert!(target.contains(&1));
        assert!(target.contains(&2));

        // handle mcollects
        let mut mcollectacks = simulation.forward_to_processes(mcollect);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the first mcollectack
        let mcommits = simulation
            .forward_to_processes(mcollectacks.pop().expect("there should be an mcollect ack"));
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mcollectack
        let mut mcommits = simulation
            .forward_to_processes(mcollectacks.pop().expect("there should be an mcollect ack"));
        // there's a commit now
        assert_eq!(mcommits.len(), 1);

        // check that the mcommit is sent to everyone
        let mcommit = mcommits.pop().expect("there should be an mcommit");
        let ToSend { target, .. } = mcommit.clone();
        assert_eq!(target.len(), n);

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mcommit);

        // check there's nothing to send
        assert!(to_sends.is_empty());

        // process 1 should have something to the executor
        let (process, executor) = simulation.get_process(process_id_1);
        let to_executor = process.to_executor();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command ready
        let mut ready = executor.handle(to_executor);
        assert_eq!(ready.len(), 1);

        // get that command
        let cmd_result = ready.pop().expect("there should a command ready");

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result, &time)
            .expect("there should a new submit");

        let (process, _) = simulation.get_process(target);
        let ToSend { msg, .. } = process.submit(cmd);
        if let Message::MStore { dot, .. } = msg {
            assert_eq!(dot, Dot::new(process_id_1, 2));
        } else {
            panic!("Message::MStore not found!");
        }
    }
}
