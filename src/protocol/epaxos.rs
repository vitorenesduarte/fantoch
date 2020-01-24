use crate::command::Command;
use crate::config::Config;
use crate::executor::{Executor, GraphExecutor};
use crate::id::{Dot, ProcessId};
use crate::protocol::common::{
    graph::{KeysClocks, QuorumClocks},
    info::{Commands, Info},
    synod::{Synod, SynodMessage},
};
use crate::protocol::{BaseProcess, MessageDot, Protocol, ToSend};
use crate::util;
use crate::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;
use std::mem;
use threshold::VClock;

type ExecutionInfo = <GraphExecutor as Executor>::ExecutionInfo;

pub struct EPaxos {
    bp: BaseProcess,
    keys_clocks: KeysClocks,
    cmds: Commands<CommandInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl Protocol for EPaxos {
    type Message = Message;
    type Executor = GraphExecutor;

    /// Creates a new `Atlas` process.
    fn new(process_id: ProcessId, config: Config) -> Self {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) = config.epaxos_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(process_id, config, fast_quorum_size, write_quorum_size);
        let keys_clocks = KeysClocks::new(config.n());
        let f = Self::allowed_faults(config.n());
        let cmds = Commands::new(process_id, config.n(), f, fast_quorum_size);
        let to_executor = Vec::new();

        // create `EPaxos`
        Self {
            bp,
            keys_clocks,
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
    fn submit(&mut self, dot: Option<Dot>, cmd: Command) -> ToSend<Self::Message> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> Option<ToSend<Message>> {
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

    fn parallel(&self) -> bool {
        self.bp.config.parallel_protocol()
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        mem::take(&mut self.to_executor)
    }

    fn show_metrics(&self) {
        self.bp.show_metrics();
    }
}

impl EPaxos {
    /// EPaxos always tolerates a minority of faults.
    pub fn allowed_faults(n: usize) -> usize {
        n / 2
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) -> ToSend<Message> {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // wrap command
        let cmd = Some(cmd);

        // compute its clock
        // - similarly to Atlas, here we shouldn't save the command in `keys_clocks`; if we do, it
        //   will be declared as a dependency of itself when this message is handled by its own
        //   coordinator, which prevents fast paths with f > 1
        let clock = self.keys_clocks.clock(&cmd);

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        ToSend {
            from: self.id(),
            target,
            msg: mcollect,
        }
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Option<Command>,
        quorum: HashSet<ProcessId>,
        remote_clock: VClock<ProcessId>,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MCollect({:?}, {:?}, {:?}) from {}",
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
            return None;
        }

        // optimization: compute clock if not from self
        let clock = if from == self.bp.process_id {
            remote_clock
        } else {
            self.keys_clocks.clock_with_past(&cmd, remote_clock)
        };

        // save command in order to be declared as a conflict for following commands
        self.keys_clocks.add(dot, &cmd);

        // update command info
        info.status = Status::COLLECT;
        info.quorum = BTreeSet::from_iter(quorum);
        // create and set consensus value
        let value = ConsensusValue::with(cmd, clock.clone());
        assert!(info.synod.maybe_set_value(|| value));

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck { dot, clock };
        let target = singleton![from];

        // return `ToSend`
        Some(ToSend {
            from: self.id(),
            target,
            msg: mcollectack,
        })
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: VClock<ProcessId>,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MCollectAck({:?}, {:?}) from {}",
            self.id(),
            dot,
            clock,
            from
        );

        // ignore ack from self (see `CommandInfo::new` for the reason why)
        if from == self.bp.process_id {
            return None;
        }

        // get cmd info
        let info = self.cmds.get(dot);

        // do nothing if we're no longer COLLECT
        if info.status != Status::COLLECT {
            return None;
        }

        // update quorum clocks
        info.quorum_clocks.add(from, clock);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // compute the union while checking whether all clocks reported are equal
            let (final_clock, all_equal) = info.quorum_clocks.union();

            // create consensus value
            // TODO can the following be more performant or at least more ergonomic?
            let cmd = info.synod.value().clone().cmd;
            let value = ConsensusValue::with(cmd, final_clock);

            // fast path condition:
            // - all reported clocks if `max_clock` was reported by at least f processes
            if all_equal {
                self.bp.fast_path();
                // fast path: create `MCommit`
                // TODO create a slim-MCommit that only sends the payload to the non-fast-quorum
                // members, or send the payload to all in a slim-MConsensus
                let mcommit = Message::MCommit { dot, value };
                let target = self.bp.all();

                // return `ToSend`
                Some(ToSend {
                    from: self.id(),
                    target,
                    msg: mcommit,
                })
            } else {
                self.bp.slow_path();
                // slow path: create `MConsensus`
                let ballot = info.synod.skip_prepare();
                let mconsensus = Message::MConsensus { dot, ballot, value };
                let target = self.bp.write_quorum();
                // return `ToSend`
                Some(ToSend {
                    from: self.id(),
                    target,
                    msg: mconsensus,
                })
            }
        } else {
            None
        }
    }

    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        value: ConsensusValue,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MCommit({:?}, {:?})", self.id(), dot, value.clock);

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return None;
        }

        // update command info:
        info.status = Status::COMMIT;

        // handle commit in synod
        let msg = SynodMessage::MChosen(value.clone());
        info.synod.handle(from, msg);

        // create execution info if not a noop
        if let Some(cmd) = value.cmd {
            // create execution info
            let execution_info = ExecutionInfo::new(dot, cmd, value.clock);
            self.to_executor.push(execution_info);
        }

        // nothing to send
        None
    }

    fn handle_mconsensus(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MConsensus({:?}, {}, {:?})",
            self.id(),
            dot,
            ballot,
            value.clock
        );

        // get cmd info
        let info = self.cmds.get(dot);

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
                return None;
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensus handler"
            ),
        };

        // create target
        let target = singleton![from];

        // return `ToSend`
        Some(ToSend {
            from: self.id(),
            target,
            msg,
        })
    }

    fn handle_mconsensusack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        ballot: u64,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MConsensusAck({:?}, {})", self.id(), dot, ballot);

        // get cmd info
        let info = self.cmds.get(dot);

        // compute message: that can either be nothing or an mcommit
        match info.synod.handle(from, SynodMessage::MAccepted(ballot)) {
            Some(SynodMessage::MChosen(value)) => {
                // enough accepts were gathered and the value has been chosen
                // create `MCommit` and target
                // create target
                let target = self.bp.all();
                let mcommit = Message::MCommit { dot, value };

                // return `ToSend`
                Some(ToSend {
                    from: self.id(),
                    target,
                    msg: mcommit,
                })
            }
            None => {
                // not enough accepts yet
                None
            }
            _ => panic!(
                "no other type of message should be output by Synod in the MConsensusAck handler"
            ),
        }
    }
}

// consensus value is a pair where the first component is the command (noop if `None`) and the
// second component its dependencies represented as a vector clock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    quorum: BTreeSet<ProcessId>, // this should be a `BTreeSet` so that `==` works in recovery
    synod: Synod<ConsensusValue>,
    // `quorum_clocks` is used by the coordinator to compute the threshold clock when deciding
    // whether to take the fast path
    quorum_clocks: QuorumClocks,
}

impl Info for CommandInfo {
    fn new(process_id: ProcessId, n: usize, f: usize, fast_quorum_size: usize) -> Self {
        // create bottom consensus value
        let initial_value = ConsensusValue::new(n);

        // although the fast quorum size is `fast_quorum_size`, we're going to initialize
        // `QuorumClocks` with `fast_quorum_size - 1` since the clock reported by the coordinator
        // shouldn't be considered in the fast path condition, and this clock is not necessary for
        // correctness; for this to work, `MCollectAck`'s from self should be ignored, or not even
        // created.
        Self {
            status: Status::START,
            quorum: BTreeSet::new(),
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            quorum_clocks: QuorumClocks::new(fast_quorum_size - 1),
        }
    }
}

// `Atlas` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    MCollect {
        dot: Dot,
        cmd: Option<Command>, // it's never a noop though
        clock: VClock<ProcessId>,
        quorum: HashSet<ProcessId>,
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

impl MessageDot for Message {}

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
    use crate::executor::ExecutorResult;
    use crate::planet::{Planet, Region};
    use crate::sim::Simulation;
    use crate::time::SimTime;

    #[test]
    fn epaxos_flow() {
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
        let executor_1 = GraphExecutor::new(config);
        let executor_2 = GraphExecutor::new(config);
        let executor_3 = GraphExecutor::new(config);

        // epaxos
        let mut epaxos_1 = EPaxos::new(process_id_1, config);
        let mut epaxos_2 = EPaxos::new(process_id_2, config);
        let mut epaxos_3 = EPaxos::new(process_id_3, config);

        // discover processes in all epaxos
        let sorted = util::sort_processes_by_distance(&europe_west2, &planet, processes.clone());
        epaxos_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(&europe_west3, &planet, processes.clone());
        epaxos_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(&us_west1, &planet, processes.clone());
        epaxos_3.discover(sorted);

        // register processes
        simulation.register_process(epaxos_1, executor_1);
        simulation.register_process(epaxos_2, executor_2);
        simulation.register_process(epaxos_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // create client 1 that is connected to epaxos 1
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

        // check that `target` is epaxos 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in epaxos 1
        let (process, executor) = simulation.get_process(target);
        executor.register(cmd.rifl(), cmd.key_count());
        let mcollect = process.submit(None, cmd);

        // check that the mcollect is being sent to 2 processes
        let ToSend { target, .. } = mcollect.clone();
        assert_eq!(target.len(), 2 * f);
        assert!(target.contains(&1));
        assert!(target.contains(&2));

        // handle mcollects
        let mut mcollectacks = simulation.forward_to_processes(mcollect);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the *only* mcollectack
        // - there's a single mcollectack single the initial coordinator does not reply to itself
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
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| executor.handle(info))
            .map(|result| match result {
                ExecutorResult::Ready(result) => result,
                _ => {
                    panic!("all results should be ready since executor configured as non-parallel")
                }
            })
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
        if let Message::MCollect { dot, .. } = msg {
            assert_eq!(dot, Dot::new(process_id_1, 2));
        } else {
            panic!("Message::MCollect not found!");
        }
    }
}
