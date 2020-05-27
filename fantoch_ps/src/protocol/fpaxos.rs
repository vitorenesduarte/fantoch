use crate::executor::SlotExecutor;
use crate::protocol::common::synod::{GCTrack, MultiSynod, MultiSynodMessage};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId};
use fantoch::protocol::{
    Action, BaseProcess, MessageIndex, PeriodicEventIndex, Protocol,
    ProtocolMetrics,
};
use fantoch::time::SysTime;
use fantoch::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;
use tracing::instrument;

type ExecutionInfo = <SlotExecutor as Executor>::ExecutionInfo;

#[derive(Clone)]
pub struct FPaxos {
    bp: BaseProcess,
    leader: ProcessId,
    multi_synod: MultiSynod<Command>,
    gc_track: GCTrack,
    to_executor: Vec<ExecutionInfo>,
}

impl Protocol for FPaxos {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = SlotExecutor;

    /// Creates a new `FPaxos` process.
    fn new(
        process_id: ProcessId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, u64)>) {
        // compute fast and write quorum sizes
        let fast_quorum_size = 0; // there's no fast quorum as we don't have fast paths
        let write_quorum_size = config.fpaxos_quorum_size();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

        // get leader from config
        let initial_leader = config.leader().expect(
            "in a leader-based protocol, the initial leader should be defined",
        );
        // create multi synod
        let multi_synod =
            MultiSynod::new(process_id, initial_leader, config.n(), config.f());
        let to_executor = Vec::new();

        // create `FPaxos`
        let protocol = Self {
            bp,
            leader: initial_leader,
            multi_synod,
            gc_track: GCTrack::new(process_id, config.n()),
            to_executor,
        };

        // create periodic events
        let gc_delay = config.garbage_collection_interval() as u64;
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
        _time: &dyn SysTime,
    ) -> Action<Message> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
        _time: &dyn SysTime,
    ) -> Action<Message> {
        match msg {
            Message::MForwardSubmit { cmd } => self.handle_submit(None, cmd),
            Message::MSpawnCommander { ballot, slot, cmd } => {
                self.handle_mspawn_commander(from, ballot, slot, cmd)
            }
            Message::MAccept { ballot, slot, cmd } => {
                self.handle_maccept(from, ballot, slot, cmd)
            }
            Message::MAccepted { ballot, slot } => {
                self.handle_maccepted(from, ballot, slot)
            }
            Message::MChosen { slot, cmd } => self.handle_mchosen(slot, cmd),
            Message::MGarbageCollection { committed } => {
                self.handle_mgc(from, committed)
            }
        }
    }

    /// Handles periodic local events.
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
        _time: &dyn SysTime,
    ) -> Vec<Action<Message>> {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection()
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

    fn leaderless() -> bool {
        false
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl FPaxos {
    /// Handles a submit operation by a client.
    #[instrument(skip(self, _dot, cmd))]
    fn handle_submit(
        &mut self,
        _dot: Option<Dot>,
        cmd: Command,
    ) -> Action<Message> {
        match self.multi_synod.submit(cmd) {
            MultiSynodMessage::MSpawnCommander(ballot, slot, cmd) => {
                // in this case, we're the leader:
                // - send a spawn commander to self (that can run in a different
                //   process for parallelism)
                let mspawn = Message::MSpawnCommander { ballot, slot, cmd };

                // return `ToSend`
                Action::ToForward { msg: mspawn }
            }
            MultiSynodMessage::MForwardSubmit(cmd) => {
                // in this case, we're not the leader and should forward the
                // command to the leader
                let mforward = Message::MForwardSubmit { cmd };
                let target = singleton![self.leader];

                // return `ToSend`
                Action::ToSend {
                    target,
                    msg: mforward,
                }
            }
            msg => panic!("can't handle {:?} in handle_submit", msg),
        }
    }

    #[instrument(skip(self, ballot, slot, cmd))]
    fn handle_mspawn_commander(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        cmd: Command,
    ) -> Action<Message> {
        log!(
            "p{}: MSpawnCommander({:?}, {:?}, {:?}) from {}",
            self.id(),
            ballot,
            slot,
            cmd,
            from
        );
        // spawn commander message should come from self
        assert_eq!(from, self.id());

        // in this case, we're the leader:
        // - handle spawn
        // - create an maccept and send it to the write quorum
        let maccept = self.multi_synod.handle(from, MultiSynodMessage::MSpawnCommander(ballot, slot, cmd)).expect("handling an MSpawnCommander in the local MultiSynod should output an MAccept");

        match maccept {
            MultiSynodMessage::MAccept(ballot, slot, cmd) => {
                // create `MAccept`
                let maccept = Message::MAccept { ballot, slot, cmd };
                let target = self.bp.write_quorum();

                // return `ToSend`
                Action::ToSend {
                    target,
                    msg: maccept,
                }
            }
            msg => panic!("can't handle {:?} in handle_mspawn_commander", msg),
        }
    }

    #[instrument(skip(self, ballot, slot, cmd))]
    fn handle_maccept(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        cmd: Command,
    ) -> Action<Message> {
        log!(
            "p{}: MAccept({:?}, {:?}, {:?}) from {}",
            self.id(),
            ballot,
            slot,
            cmd,
            from
        );

        if let Some(msg) = self
            .multi_synod
            .handle(from, MultiSynodMessage::MAccept(ballot, slot, cmd))
        {
            match msg {
                MultiSynodMessage::MAccepted(ballot, slot) => {
                    // create `MAccepted` and target
                    let maccepted = Message::MAccepted { ballot, slot };
                    let target = singleton![from];

                    // return `ToSend`
                    Action::ToSend {
                        target,
                        msg: maccepted,
                    }
                }
                msg => panic!("can't handle {:?} in handle_maccept", msg),
            }
        } else {
            // TODO maybe warn the leader that it is not longer a leader?
            Action::Nothing
        }
    }

    #[instrument(skip(self, ballot, slot))]
    fn handle_maccepted(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
    ) -> Action<Message> {
        log!(
            "p{}: MAccepted({:?}, {:?}) from {}",
            self.id(),
            ballot,
            slot,
            from
        );

        if let Some(msg) = self
            .multi_synod
            .handle(from, MultiSynodMessage::MAccepted(ballot, slot))
        {
            match msg {
                MultiSynodMessage::MChosen(slot, cmd) => {
                    // create `MChosen`
                    let mcommit = Message::MChosen { slot, cmd };
                    let target = self.bp.all();

                    // return `ToSend`
                    return Action::ToSend {
                        target,
                        msg: mcommit,
                    };
                }
                msg => panic!("can't handle {:?} in handle_maccepted", msg),
            }
        }

        // nothing to send
        Action::Nothing
    }

    #[instrument(skip(self, slot, cmd))]
    fn handle_mchosen(&mut self, slot: u64, cmd: Command) -> Action<Message> {
        log!("p{}: MCommit({:?}, {:?})", self.id(), slot, cmd);

        // create execution info
        let execution_info = ExecutionInfo::new(slot, cmd);
        self.to_executor.push(execution_info);

        // register that it has been committed
        self.gc_track.commit(slot);

        // nothing to send
        Action::Nothing
    }

    #[instrument(skip(self, from, committed))]
    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: u64,
    ) -> Action<Message> {
        log!(
            "p{}: MGarbageCollection({:?}) from {}",
            self.id(),
            committed,
            from
        );
        self.gc_track.committed_by(from, committed);
        // perform garbage collection of stable slots
        let stable = self.gc_track.stable();
        let stable_count = self.multi_synod.gc(stable);
        self.bp.stable(stable_count);
        Action::Nothing
    }

    #[instrument(skip(self))]
    fn handle_event_garbage_collection(&mut self) -> Vec<Action<Message>> {
        log!("p{}: PeriodicEvent::GarbageCollection", self.id());

        // retrieve the committed slot
        let committed = self.gc_track.committed();

        // create `ToSend`
        let tosend = Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        };

        vec![tosend]
    }
}

// `FPaxos` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    MForwardSubmit {
        cmd: Command,
    },
    MSpawnCommander {
        ballot: u64,
        slot: u64,
        cmd: Command,
    },
    MAccept {
        ballot: u64,
        slot: u64,
        cmd: Command,
    },
    MAccepted {
        ballot: u64,
        slot: u64,
    },
    MChosen {
        slot: u64,
        cmd: Command,
    },
    MGarbageCollection {
        committed: u64,
    },
}

const LEADER_WORKER_INDEX: usize = fantoch::run::LEADER_WORKER_INDEX;
const ACCEPTOR_WORKER_INDEX: usize = 1;

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{worker_index_no_shift, worker_index_shift};
        match self {
            Self::MForwardSubmit { .. } => {
                // forward commands to the leader worker
                worker_index_no_shift(LEADER_WORKER_INDEX)
            }
            Self::MAccept { .. } => {
                // forward accepts to the acceptor worker
                worker_index_no_shift(ACCEPTOR_WORKER_INDEX)
            }
            Self::MChosen { .. } => {
                // forward chosen messages also to acceptor worker:
                // - at point we had a learner worker, but since the acceptor
                //   needs to know about committed slows to perform GC, we
                //   wouldn't gain much (if anything) in separating these roles
                worker_index_no_shift(ACCEPTOR_WORKER_INDEX)
            }
            // spawn commanders and accepted messages should be forwarded to
            // the commander process:
            // - make sure that these commanders are never spawned in the
            //   previous 2 workers
            Self::MSpawnCommander { slot, .. } => {
                worker_index_shift(*slot as usize)
            }
            Self::MAccepted { slot, .. } => {
                worker_index_shift(*slot as usize)
            }
            Self::MGarbageCollection { .. } => {
                // since it's the acceptor that contains the slots to be gc-ed,
                // we should simply run gc-tracking there as well:
                // - this removes the need for Message::MStable seen in the
                //   other implementations
                worker_index_no_shift(ACCEPTOR_WORKER_INDEX)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl PeriodicEventIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::worker_index_no_shift;
        match self {
            Self::GarbageCollection => {
                worker_index_no_shift(ACCEPTOR_WORKER_INDEX)
            }
        }
    }
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
    fn fpaxos_flow() {
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

        // set process 1 as the leader
        config.set_leader(process_id_1);

        // executors
        let executor_1 = SlotExecutor::new(process_id_1, config);
        let executor_2 = SlotExecutor::new(process_id_2, config);
        let executor_3 = SlotExecutor::new(process_id_3, config);

        // fpaxos
        let (mut fpaxos_1, _) = FPaxos::new(process_id_1, config);
        let (mut fpaxos_2, _) = FPaxos::new(process_id_2, config);
        let (mut fpaxos_3, _) = FPaxos::new(process_id_3, config);

        // discover processes in all fpaxos
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        fpaxos_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        fpaxos_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        fpaxos_3.discover(sorted);

        // register processes
        simulation.register_process(fpaxos_1, executor_1);
        simulation.register_process(fpaxos_2, executor_2);
        simulation.register_process(fpaxos_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

        // create client 1 that is connected to fpaxos 1
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

        // check that `target` is fpaxos 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in fpaxos 1
        let (process, executor, time) = simulation.get_process(target);
        executor.wait_for(&cmd);
        let spawn = process.submit(None, cmd, time);

        // check that the register created a spawn commander to self and handle
        // it locally
        let maccept = if let Action::ToForward { msg } = spawn {
            process.handle(process_id_1, msg, time)
        } else {
            panic!("Action::ToForward not found!");
        };

        // check that the maccept is being sent to 2 processes
        let check_target = |target: &HashSet<u64>| {
            target.len() == f + 1 && target.contains(&1) && target.contains(&2)
        };
        assert!(
            matches!(maccept.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle maccepts
        let mut maccepted =
            simulation.forward_to_processes((process_id_1, maccept));

        // check that there are 2 maccepted
        assert_eq!(maccepted.len(), 2 * f);

        // handle the first maccepted
        let mchosen = simulation.forward_to_processes(
            maccepted.pop().expect("there should be an maccepted"),
        );
        // no mchosen yet
        assert!(mchosen.is_empty());

        // handle the second macceptack
        let mut mchosen = simulation.forward_to_processes(
            maccepted.pop().expect("there should be an maccepted"),
        );
        // there's an mchosen now
        assert_eq!(mchosen.len(), 1);

        // check that the mchosen is sent to everyone
        let mchosen = mchosen.pop().expect("there should be an mcommit");
        let check_target = |target: &HashSet<u64>| target.len() == n;
        assert!(
            matches!(mchosen.clone(), (_, Action::ToSend {target, ..}) if check_target(&target))
        );

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mchosen);

        // check there's nothing to send
        assert!(to_sends.is_empty());

        // process 1 should have something to the executor
        let (process, executor, _) = simulation.get_process(process_id_1);
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
            .forward_to_client(cmd_result)
            .expect("there should a new submit");

        let (process, _, time) = simulation.get_process(target);
        let action = process.submit(None, cmd, time);
        let check_msg = |msg: &Message| matches!(msg, Message::MSpawnCommander{slot, ..} if slot == &2);
        assert!(matches!(action, Action::ToForward {msg} if check_msg(&msg)));
    }
}
