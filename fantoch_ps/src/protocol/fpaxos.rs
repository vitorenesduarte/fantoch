use crate::executor::SlotExecutor;
use crate::protocol::common::synod::{GCTrack, MultiSynod, MultiSynodMessage};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{
    Action, BaseProcess, MessageIndex, Protocol, ProtocolMetrics,
};
use fantoch::time::SysTime;
use fantoch::{singleton, trace};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::time::Duration;

type ExecutionInfo = <SlotExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FPaxos {
    bp: BaseProcess,
    leader: ProcessId,
    multi_synod: MultiSynod<Command>,
    gc_track: GCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
}

impl Protocol for FPaxos {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = SlotExecutor;

    /// Creates a new `FPaxos` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let fast_quorum_size = 0; // there's no fast quorum as we don't have fast paths
        let write_quorum_size = config.fpaxos_quorum_size();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
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
        let to_processes = Vec::new();
        let to_executors = Vec::new();

        // create `FPaxos`
        let protocol = Self {
            bp,
            leader: initial_leader,
            multi_synod,
            gc_track: GCTrack::new(process_id, config.n()),
            to_processes,
            to_executors,
        };

        // create periodic events
        let events = if let Some(interval) = config.gc_interval() {
            vec![(PeriodicEvent::GarbageCollection, interval)]
        } else {
            vec![]
        };

        // return both
        (protocol, events)
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Returns the shard identifier.
    fn shard_id(&self) -> ShardId {
        self.bp.shard_id
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    fn discover(
        &mut self,
        processes: Vec<(ProcessId, ShardId)>,
    ) -> (bool, HashMap<ShardId, ProcessId>) {
        let connect_ok = self.bp.discover(processes);
        (connect_ok, self.bp.closest_shard_process().clone())
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, dot: Option<Dot>, cmd: Command, _time: &dyn SysTime) {
        self.handle_submit(dot, cmd);
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        _from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) {
        match msg {
            Message::MForwardSubmit { cmd } => self.handle_submit(None, cmd),
            Message::MSpawnCommander { ballot, slot, cmd } => {
                self.handle_mspawn_commander(from, ballot, slot, cmd, time)
            }
            Message::MAccept { ballot, slot, cmd } => {
                self.handle_maccept(from, ballot, slot, cmd, time)
            }
            Message::MAccepted { ballot, slot } => {
                self.handle_maccepted(from, ballot, slot, time)
            }
            Message::MChosen { slot, cmd } => {
                self.handle_mchosen(slot, cmd, time)
            }
            Message::MGarbageCollection { committed } => {
                self.handle_mgc(from, committed, time)
            }
        }
    }

    /// Handles periodic local events.
    fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime) {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
        }
    }

    /// Returns a new action to be sent to other processes.
    fn to_processes(&mut self) -> Option<Action<Self>> {
        self.to_processes.pop()
    }

    /// Returns new execution info for executors.
    fn to_executors(&mut self) -> Option<ExecutionInfo> {
        self.to_executors.pop()
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
    // #[instrument(skip(self, _dot, cmd))]
    fn handle_submit(&mut self, _dot: Option<Dot>, cmd: Command) {
        match self.multi_synod.submit(cmd) {
            MultiSynodMessage::MSpawnCommander(ballot, slot, cmd) => {
                // in this case, we're the leader:
                // - send a spawn commander to self (that can run in a different
                //   process for parallelism)
                let mspawn = Message::MSpawnCommander { ballot, slot, cmd };

                // save new action
                self.to_processes.push(Action::ToForward { msg: mspawn });
            }
            MultiSynodMessage::MForwardSubmit(cmd) => {
                // in this case, we're not the leader and should forward the
                // command to the leader
                let mforward = Message::MForwardSubmit { cmd };
                let target = singleton![self.leader];

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mforward,
                });
            }
            msg => panic!("can't handle {:?} in handle_submit", msg),
        }
    }

    // #[instrument(skip(self, ballot, slot, cmd, _time))]
    fn handle_mspawn_commander(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        cmd: Command,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MSpawnCommander({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            ballot,
            slot,
            cmd,
            from,
            _time.micros()
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

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: maccept,
                });
            }
            msg => panic!("can't handle {:?} in handle_mspawn_commander", msg),
        }
    }

    // #[instrument(skip(self, ballot, slot, cmd, _time))]
    fn handle_maccept(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        cmd: Command,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MAccept({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            ballot,
            slot,
            cmd,
            from,
            _time.micros()
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

                    // save new action
                    self.to_processes.push(Action::ToSend {
                        target,
                        msg: maccepted,
                    });
                }
                msg => panic!("can't handle {:?} in handle_maccept", msg),
            }
        } else {
            // TODO maybe warn the leader that it is not longer a leader?
        }
    }

    // #[instrument(skip(self, ballot, slot, _time))]
    fn handle_maccepted(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MAccepted({:?}, {:?}) from {} | time={}",
            self.id(),
            ballot,
            slot,
            from,
            _time.micros()
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

                    // save new action
                    self.to_processes.push(Action::ToSend {
                        target,
                        msg: mcommit,
                    });
                }
                msg => panic!("can't handle {:?} in handle_maccepted", msg),
            }
        }
    }

    // #[instrument(skip(self, slot, cmd, _time))]
    fn handle_mchosen(&mut self, slot: u64, cmd: Command, _time: &dyn SysTime) {
        trace!(
            "p{}: MCommit({:?}, {:?}) | time={}",
            self.id(),
            slot,
            cmd,
            _time.micros()
        );

        // create execution info
        let execution_info = ExecutionInfo::new(slot, cmd);
        self.to_executors.push(execution_info);

        if self.gc_running() {
            // register that it has been committed
            self.gc_track.commit(slot);
        } else {
            // if we're not running gc, remove the slot info now
            self.multi_synod.gc_single(slot);
        }
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }

    // #[instrument(skip(self, from, committed, _time))]
    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: u64,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            _time.micros()
        );
        self.gc_track.committed_by(from, committed);
        // perform garbage collection of stable slots
        let stable = self.gc_track.stable();
        let stable_count = self.multi_synod.gc(stable);
        self.bp.stable(stable_count);
    }

    // #[instrument(skip(self, _time))]
    fn handle_event_garbage_collection(&mut self, _time: &dyn SysTime) {
        trace!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.micros()
        );

        // retrieve the committed slot
        let committed = self.gc_track.committed();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        })
    }
}

// `FPaxos` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            Self::MAccepted { slot, .. } => worker_index_shift(*slot as usize),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl MessageIndex for PeriodicEvent {
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
    use fantoch::client::{Client, KeyGen, ShardGen, Workload};
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

        // there's a single shard
        let shard_id = 0;

        // processes
        let processes = vec![
            (process_id_1, shard_id, europe_west2.clone()),
            (process_id_2, shard_id, europe_west3.clone()),
            (process_id_3, shard_id, us_west1.clone()),
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
        let executor_1 = SlotExecutor::new(process_id_1, shard_id, config);
        let executor_2 = SlotExecutor::new(process_id_2, shard_id, config);
        let executor_3 = SlotExecutor::new(process_id_3, shard_id, config);

        // fpaxos
        let (mut fpaxos_1, _) = FPaxos::new(process_id_1, shard_id, config);
        let (mut fpaxos_2, _) = FPaxos::new(process_id_2, shard_id, config);
        let (mut fpaxos_3, _) = FPaxos::new(process_id_3, shard_id, config);

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
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shard_count: 1 };
        let keys_per_shard = 1;
        let key_gen = KeyGen::ConflictRate { conflict_rate: 100 };
        let total_commands = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );

        // create client 1 that is connected to fpaxos 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let status_frequency = None;
        let mut client_1 = Client::new(client_id, workload, status_frequency);

        // discover processes in client 1
        let closest =
            util::closest_process_per_shard(&client_region, &planet, processes);
        client_1.connect(closest);

        // start client
        let (target_shard, cmd) = client_1
            .next_cmd(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is fpaxos 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in fpaxos 1
        let (process, _, pending, time) = simulation.get_process(target);
        pending.wait_for(&cmd);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let spawn = actions.pop().unwrap();

        // check that the register created a spawn commander to self and handle
        // it locally
        let mut actions: Vec<_> = if let Action::ToForward { msg } = spawn {
            process.handle(process_id_1, shard_id, msg, time);
            process.to_processes_iter().collect()
        } else {
            panic!("Action::ToForward not found!");
        };
        // there's a single action
        assert_eq!(actions.len(), 1);
        let maccept = actions.pop().unwrap();

        // check that the maccept is being sent to 2 processes
        let check_target = |target: &HashSet<ProcessId>| {
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
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mchosen.clone(), (_, Action::ToSend {target, ..}) if check_target(&target))
        );

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mchosen);

        // check there's nothing to send
        assert!(to_sends.is_empty());

        // process 1 should have something to the executor
        let (process, executor, pending, time) =
            simulation.get_process(process_id_1);
        let to_executor: Vec<_> = process.to_executors_iter().collect();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command partial
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| {
                executor.handle(info, time);
                executor.to_clients_iter().collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ready.len(), 1);

        // get that command
        let executor_result =
            ready.pop().expect("there should an executor result");
        let cmd_result = pending
            .add_executor_result(executor_result)
            .expect("there should be a command result");

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result)
            .expect("there should a new submit");

        let (process, _, _, time) = simulation.get_process(target);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MSpawnCommander{slot, ..} if slot == &2);
        assert!(matches!(mcollect, Action::ToForward {msg} if check_msg(&msg)));
    }
}
