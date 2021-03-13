use crate::command::Command;
use crate::config::Config;
use crate::executor::{BasicExecutionInfo, BasicExecutor, Executor};
use crate::id::{Dot, ProcessId, ShardId};
use crate::protocol::{
    Action, BaseProcess, GCTrack, Info, MessageIndex, Protocol,
    ProtocolMetrics, SequentialCommandsInfo,
};
use crate::singleton;
use crate::time::SysTime;
use crate::trace;
use crate::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use threshold::VClock;

type ExecutionInfo = <BasicExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Basic {
    bp: BaseProcess,
    cmds: SequentialCommandsInfo<BasicInfo>,
    gc_track: GCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
    buffered_mcommits: HashSet<Dot>,
}

impl Protocol for Basic {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = BasicExecutor;

    /// Creates a new `Basic` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let fast_quorum_size = config.basic_quorum_size();
        let write_quorum_size = 0; // there's no write quorum as we have 100% fast paths

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let cmds = SequentialCommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            config.f(),
            fast_quorum_size,
            write_quorum_size,
        );
        let gc_track = GCTrack::new(process_id, shard_id, config.n());
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let buffered_mcommits = HashSet::new();

        // create `Basic`
        let protocol = Self {
            bp,
            cmds,
            gc_track,
            to_processes,
            to_executors,
            buffered_mcommits,
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
        _time: &dyn SysTime,
    ) {
        match msg {
            Message::MStore { dot, cmd, quorum } => {
                self.handle_mstore(from, dot, cmd, quorum)
            }
            Message::MStoreAck { dot } => self.handle_mstoreack(from, dot),
            Message::MCommit { dot } => self.handle_mcommit(dot),
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
        _time: &dyn SysTime,
    ) {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection()
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
        true
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl Basic {
    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // create `MStore` and target
        let quorum = self.bp.fast_quorum();
        let mstore = Message::MStore { dot, cmd, quorum };
        let target = self.bp.all();

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mstore,
        })
    }

    fn handle_mstore(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
    ) {
        trace!(
            "p{}: MStore({:?}, {:?}, {:?}) from {}",
            self.id(),
            dot,
            cmd,
            quorum,
            from
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // update command info
        info.cmd = Some(cmd);

        // reply if we're part of the quorum
        if quorum.contains(&self.id()) {
            // create `MStoreAck` and target
            let mstoreack = Message::MStoreAck { dot };
            let target = singleton![from];

            // save new action
            self.to_processes.push(Action::ToSend {
                target,
                msg: mstoreack,
            })
        }

        // check if there's a buffered commit notification; if yes, handle
        // the commit again (since now we have the payload)
        if self.buffered_mcommits.remove(&dot) {
            self.handle_mcommit(dot);
        }
    }

    fn handle_mstoreack(&mut self, from: ProcessId, dot: Dot) {
        trace!("p{}: MStoreAck({:?}) from {}", self.id(), dot, from);

        // get cmd info
        let info = self.cmds.get(dot);

        // update quorum clocks
        info.acks.insert(from);

        // check if we have all necessary replies
        if info.acks.len() == self.bp.config.basic_quorum_size() {
            let mcommit = Message::MCommit { dot };
            let target = self.bp.all();

            // save new action
            self.to_processes.push(Action::ToSend {
                target,
                msg: mcommit,
            });
        }
    }

    fn handle_mcommit(&mut self, dot: Dot) {
        trace!("p{}: MCommit({:?})", self.id(), dot);

        // get cmd info and its rifl
        let info = self.cmds.get(dot);

        // check if we have received the initial `MStore`
        if let Some(cmd) = info.cmd.as_ref() {
            // if so, create execution info:
            // - one entry per key being accessed will be created, which allows
            //   the basic executor to run in parallel
            let rifl = cmd.rifl();
            let execution_info =
                cmd.iter(self.bp.shard_id).map(|(key, ops)| {
                    BasicExecutionInfo::new(rifl, key.clone(), ops.clone())
                });
            self.to_executors.extend(execution_info);

            if self.gc_running() {
                // notify self with the committed dot
                self.to_processes.push(Action::ToForward {
                    msg: Message::MCommitDot { dot },
                });
            } else {
                // if we're not running gc, remove the dot info now
                self.cmds.gc_single(dot);
            }
        } else {
            // if not, buffer this `MCommit` notification
            self.buffered_mcommits.insert(dot);
        }
    }

    fn handle_mcommit_dot(&mut self, from: ProcessId, dot: Dot) {
        trace!("p{}: MCommitDot({:?})", self.id(), dot);
        assert_eq!(from, self.bp.process_id);
        self.gc_track.add_to_clock(dot);
    }

    fn handle_mgc(&mut self, from: ProcessId, committed: VClock<ProcessId>) {
        trace!(
            "p{}: MGarbageCollection({:?}) from {}",
            self.id(),
            committed,
            from
        );
        self.gc_track.update_clock_of(from, committed);
        // compute newly stable dots
        let stable = self.gc_track.stable();
        // create `ToForward` to self
        if !stable.is_empty() {
            self.to_processes.push(Action::ToForward {
                msg: Message::MStable { stable },
            })
        }
    }

    fn handle_mstable(
        &mut self,
        from: ProcessId,
        stable: Vec<(ProcessId, u64, u64)>,
    ) {
        trace!("p{}: MStable({:?}) from {}", self.id(), stable, from);
        assert_eq!(from, self.bp.process_id);
        let stable_count = self.cmds.gc(stable);
        self.bp.stable(stable_count);
    }

    fn handle_event_garbage_collection(&mut self) {
        trace!("p{}: PeriodicEvent::GarbageCollection", self.id());

        // retrieve the committed clock
        let committed = self.gc_track.clock().frontier();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }
}

// `BasicInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone, PartialEq, Eq)]
struct BasicInfo {
    cmd: Option<Command>,
    acks: HashSet<ProcessId>,
}

impl Info for BasicInfo {
    fn new(
        _process_id: ProcessId,
        _shard_id: ShardId,
        _n: usize,
        _f: usize,
        fast_quorum_size: usize,
        _write_quorum_size: usize,
    ) -> Self {
        // create bottom consensus value
        Self {
            cmd: None,
            acks: HashSet::with_capacity(fast_quorum_size),
        }
    }
}

// `Basic` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Message {
    MStore {
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
    },
    MStoreAck {
        dot: Dot,
    },
    MCommit {
        dot: Dot,
    },
    MCommitDot {
        dot: Dot,
    },
    MGarbageCollection {
        committed: VClock<ProcessId>,
    },
    MStable {
        stable: Vec<(ProcessId, u64, u64)>,
    },
}

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use crate::load_balance::{
            worker_dot_index_shift, worker_index_no_shift, GC_WORKER_INDEX,
        };
        match self {
            // Protocol messages
            Self::MStore { dot, .. } => worker_dot_index_shift(&dot),
            Self::MStoreAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            // GC messages
            Self::MCommitDot { .. } => worker_index_no_shift(GC_WORKER_INDEX),
            Self::MGarbageCollection { .. } => {
                worker_index_no_shift(GC_WORKER_INDEX)
            }
            Self::MStable { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl MessageIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use crate::load_balance::{worker_index_no_shift, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, KeyGen, Workload};
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
        let config = Config::new(n, f);

        // executors
        let executor_1 = BasicExecutor::new(process_id_1, shard_id, config);
        let executor_2 = BasicExecutor::new(process_id_2, shard_id, config);
        let executor_3 = BasicExecutor::new(process_id_3, shard_id, config);

        // basic
        let (mut basic_1, _) = Basic::new(process_id_1, shard_id, config);
        let (mut basic_2, _) = Basic::new(process_id_2, shard_id, config);
        let (mut basic_3, _) = Basic::new(process_id_3, shard_id, config);

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
        let shard_count = 1;
        let keys_per_command = 1;
        let conflict_rate = 100;
        let pool_size = 1;
        let key_gen = KeyGen::ConflictPool {
            conflict_rate,
            pool_size,
        };
        let commands_per_client = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // create client 1 that is connected to basic 1
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
            .cmd_send(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is basic 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in basic 1
        let (process, _, pending, time) = simulation.get_process(process_id_1);
        pending.wait_for(&cmd);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();

        // there's a single action
        assert_eq!(actions.len(), 1);
        let mstore = actions.pop().unwrap();

        // check that the mstore is being sent to all processes
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mstore.clone(), Action::ToSend {target, ..} if check_target(&target))
        );

        // handle mstores
        let mut mstoreacks =
            simulation.forward_to_processes((process_id_1, mstore));

        // check that there are 2 mstoreacks
        assert_eq!(mstoreacks.len(), 2);

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
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mcommit.clone(), (_, Action::ToSend {target, ..}) if check_target(&target))
        );

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mcommit);

        // check the MCommitDot
        let check_msg =
            |msg: &Message| matches!(msg, Message::MCommitDot { .. });
        assert!(to_sends.into_iter().all(|(_, action)| {
            matches!(action, Action::ToForward { msg } if check_msg(&msg))
        }));

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
        let mstore = actions.pop().unwrap();
        let check_msg = |msg: &Message| matches!(msg, Message::MStore {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(matches!(mstore, Action::ToSend {msg, ..} if check_msg(&msg)));
    }
}
