use crate::command::Command;
use crate::config::Config;
use crate::executor::{BasicExecutionInfo, BasicExecutor, Executor};
use crate::id::{Dot, ProcessId};
use crate::protocol::{
    BaseProcess, CommandsInfo, Info, MessageIndex, MessageIndexes,
    PeriodicEventIndex, Protocol, ProtocolMetrics, ToSend,
};
use crate::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;
use threshold::VClock;

type ExecutionInfo = <BasicExecutor as Executor>::ExecutionInfo;

#[derive(Clone)]
pub struct Basic {
    bp: BaseProcess,
    cmds: CommandsInfo<BasicInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl Protocol for Basic {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = BasicExecutor;

    /// Creates a new `Basic` process.
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(PeriodicEvent, u64)>) {
        // compute fast and write quorum sizes
        let fast_quorum_size = config.basic_quorum_size();
        let write_quorum_size = 0; // there's no write quorum as we have 100% fast paths

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let cmds = CommandsInfo::new(
            process_id,
            config.n(),
            config.f(),
            fast_quorum_size,
        );
        let to_executor = Vec::new();

        // create `Basic`
        let protocol = Self {
            bp,
            cmds,
            to_executor,
        };

        // create periodic events
        let gc_delay = config.garbage_collection_delay() * 10000;
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
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
    ) -> ToSend<Self::Message> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
    ) -> Option<ToSend<Message>> {
        match msg {
            Message::MStore { dot, cmd } => self.handle_mstore(from, dot, cmd),
            Message::MStoreAck { dot } => self.handle_mstoreack(from, dot),
            Message::MCommit { dot, cmd } => {
                self.handle_mcommit(from, dot, cmd)
            }
            Message::MCommitDot { dot } => self.handle_mcommit_dot(from, dot),
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
    ) -> Vec<ToSend<Message>> {
        match event {
            PeriodicEvent::GarbageCollection => {
                log!("p{}: PeriodicEvent::GarbageCollection", self.id());

                // retrieve the committed clock and stable dots
                let (committed, stable) = self.cmds.committed_and_stable();

                // create `ToSend`
                let tosend = ToSend {
                    from: self.id(),
                    target: self.bp.all_but_me(),
                    msg: Message::MGarbageCollection { committed },
                };

                // create `ToSend` to self
                let toself = ToSend {
                    from: self.id(),
                    target: singleton![self.id()],
                    msg: Message::MStable { stable },
                };

                vec![tosend, toself]
            }
        }
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        mem::take(&mut self.to_executor)
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl Basic {
    /// Handles a submit operation by a client.
    fn handle_submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
    ) -> ToSend<Message> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

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

    fn handle_mstoreack(
        &mut self,
        from: ProcessId,
        dot: Dot,
    ) -> Option<ToSend<Message>> {
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

        // get cmd info and its rifl
        let info = self.cmds.get(dot);

        // update command info
        info.cmd = Some(cmd.clone());

        // create execution info:
        // - one entry per key being accessed will be created, which allows the
        //   basic executor to run in parallel
        let rifl = cmd.rifl();
        let execution_info = cmd
            .into_iter()
            .map(|(key, op)| BasicExecutionInfo::new(rifl, key, op));
        self.to_executor.extend(execution_info);

        // notify self with the committed dot
        Some(ToSend {
            from: self.id(),
            target: singleton![self.id()],
            msg: Message::MCommitDot { dot },
        })
    }

    fn handle_mcommit_dot(
        &mut self,
        from: ProcessId,
        dot: Dot,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MCommitDot({:?})", self.id(), dot);
        assert_eq!(from, self.bp.process_id);
        self.cmds.commit(dot);
        None
    }

    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MGarbageCollection({:?}) from {}",
            self.id(),
            committed,
            from
        );
        self.cmds.committed_by(from, committed);
        None
    }

    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MStable({:?}) from {}", self.id(), stable, from);
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
        None
    }
}

// `BasicInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Clone)]
struct BasicInfo {
    cmd: Option<Command>,
    missing_acks: usize,
}

impl Info for BasicInfo {
    fn new(
        _process_id: ProcessId,
        _n: usize,
        _f: usize,
        fast_quorum_size: usize,
    ) -> Self {
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
    MCommitDot { dot: Dot },
    MGarbageCollection { committed: VClock<ProcessId> },
    MStable { stable: Vec<(ProcessId, u64, u64)> },
}

impl MessageIndex for Message {
    fn index(&self) -> MessageIndexes {
        match self {
            // Protocol messages
            Self::MStore { dot, .. } => MessageIndexes::DotIndex(dot),
            Self::MStoreAck { dot, .. } => MessageIndexes::DotIndex(dot),
            Self::MCommit { dot, .. } => MessageIndexes::DotIndex(dot),
            // GC messages
            Self::MCommitDot { .. } => {
                MessageIndexes::Index(crate::run::GC_WORKER_INDEX)
            }
            Self::MGarbageCollection { .. } => {
                MessageIndexes::Index(crate::run::GC_WORKER_INDEX)
            }
            Self::MStable { .. } => MessageIndexes::None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl PeriodicEventIndex for PeriodicEvent {
    fn index(&self) -> Option<usize> {
        match self {
            Self::GarbageCollection => Some(crate::run::GC_WORKER_INDEX),
        }
    }
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
        let planet = Planet::new();

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
        let (mut basic_1, _) = Basic::new(process_id_1, config);
        let (mut basic_2, _) = Basic::new(process_id_2, config);
        let (mut basic_3, _) = Basic::new(process_id_3, config);

        // discover processes in all basic
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        basic_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        basic_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        basic_3.discover(sorted);

        // register processes
        simulation.register_process(basic_1, executor_1);
        simulation.register_process(basic_2, executor_2);
        simulation.register_process(basic_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

        // create client 1 that is connected to basic 1
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

        // check that `target` is basic 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in basic 1
        let (process, executor) = simulation.get_process(target);
        executor.wait_for(&cmd);
        let mstore = process.submit(None, cmd);

        // check that the mstore is being sent to 2 processes
        let ToSend { target, .. } = mstore.clone();
        assert_eq!(target.len(), 2 * f);
        assert!(target.contains(&1));
        assert!(target.contains(&2));

        // handle mstores
        let mut mstoreacks = simulation.forward_to_processes(mstore);

        // check that there are 2 mstoreacks
        assert_eq!(mstoreacks.len(), 2 * f);

        // handle the first mstoreack
        let mcommits = simulation.forward_to_processes(
            mstoreacks.pop().expect("there should be an mstore ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mstoreack
        let mut mcommits = simulation.forward_to_processes(
            mstoreacks.pop().expect("there should be an mstore ack"),
        );
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
        let ToSend { msg, .. } = process.submit(None, cmd);
        if let Message::MStore { dot, .. } = msg {
            assert_eq!(dot, Dot::new(process_id_1, 2));
        } else {
            panic!("Message::MStore not found!");
        }
    }
}
