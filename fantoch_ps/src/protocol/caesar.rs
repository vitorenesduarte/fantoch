use crate::executor::PredecessorsExecutor;
use crate::protocol::common::pred::{
    Clock, KeyClocks, QuorumClocks, QuorumRetries, SequentialKeyClocks,
};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::Executor;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{
    Action, BaseProcess, Executed, GCTrack, Info, LockedCommandsInfo,
    MessageIndex, Protocol, ProtocolMetrics, ProtocolMetricsKind,
};
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{singleton, trace};
use fantoch::{HashMap, HashSet};
use parking_lot::RwLockWriteGuard;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use threshold::VClock;

// TODO: Sequential -> Locked
type LockedKeyClocks = SequentialKeyClocks;
pub type CaesarSequential = Caesar<SequentialKeyClocks>;
pub type CaesarLocked = Caesar<LockedKeyClocks>;

type ExecutionInfo = <PredecessorsExecutor as Executor>::ExecutionInfo;

#[derive(Debug, Clone)]
pub struct Caesar<KC: KeyClocks> {
    bp: BaseProcess,
    key_clocks: KC,
    cmds: LockedCommandsInfo<CaesarInfo>,
    gc_track: GCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<ExecutionInfo>,
    // retry requests that arrived before the initial `MPropose` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_retries: HashMap<Dot, (ProcessId, Clock, HashSet<Dot>)>,
    // commit notifications that arrived before the initial `MPropose` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, Clock, HashSet<Dot>)>,
    wait_condition: bool,
}

impl<KC: KeyClocks> Protocol for Caesar<KC> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = PredecessorsExecutor;

    /// Creates a new `Caesar` process.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) =
            config.caesar_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_clocks = KC::new(process_id, shard_id);
        let f = Self::allowed_faults(config.n());
        let cmds = LockedCommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            f,
            fast_quorum_size,
            write_quorum_size,
        );
        let gc_track = GCTrack::new(process_id, shard_id, config.n());
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let buffered_retries = HashMap::new();
        let buffered_commits = HashMap::new();
        let wait_condition = config.caesar_wait_condition();

        // create `Caesar`
        let protocol = Self {
            bp,
            key_clocks,
            cmds,
            gc_track,
            to_processes,
            to_executors,
            buffered_retries,
            buffered_commits,
            wait_condition,
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
            Message::MPropose { dot, cmd, clock } => {
                self.handle_mpropose(from, dot, cmd, clock, time)
            }
            Message::MProposeAck {
                dot,
                clock,
                deps,
                ok,
            } => self.handle_mproposeack(from, dot, clock, deps, ok, time),
            Message::MCommit { dot, clock, deps } => {
                self.handle_mcommit(from, dot, clock, deps, time)
            }
            Message::MRetry { dot, clock, deps } => {
                self.handle_mretry(from, dot, clock, deps, time)
            }
            Message::MRetryAck { dot, deps } => {
                self.handle_mretryack(from, dot, deps, time)
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

    fn handle_executed(&mut self, executed: Executed, _time: &dyn SysTime) {
        self.gc_track.update_clock(executed);
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
        KC::parallel()
    }

    fn leaderless() -> bool {
        true
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl<KC: KeyClocks> Caesar<KC> {
    /// Caesar always tolerates a minority of faults.
    pub fn allowed_faults(n: usize) -> usize {
        n / 2
    }

    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // compute its clock
        let clock = self.key_clocks.clock_next();

        // create `MPropose` and target
        let mpropose = Message::MPropose { dot, cmd, clock };
        // here we send to everyone because we want the fastest fast quorum that
        // replies with an ok (due to the waiting condition, this fast quorum
        // may not be the closest one)
        let target = self.bp.all();

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mpropose,
        });
    }

    fn handle_mpropose(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        remote_clock: Clock,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MPropose({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from,
            time.micros()
        );

        // we use the following assumption in `Self::send_mpropose_ack`
        assert_eq!(dot.source(), from);

        // merge clocks
        self.key_clocks.clock_join(&remote_clock);

        // get cmd info
        let info_ref = self.cmds.get_or_default(dot);
        let mut info = info_ref.write();

        // discard message if no longer in START
        if info.status != Status::START {
            return;
        }

        // register start time if we're the coordinator
        if dot.source() == from {
            info.start_time_ms = Some(time.millis());
        }

        // if yes, compute set of predecessors
        let mut blocked_by = HashSet::new();
        let deps = self.key_clocks.predecessors(
            dot,
            &cmd,
            remote_clock,
            Some(&mut blocked_by),
        );

        // update command info
        info.status = Status::PROPOSE;
        info.cmd = Some(cmd);
        info.deps = deps;
        Self::update_clock(&mut self.key_clocks, dot, &mut info, remote_clock);

        // save command's clock and update `blocked_by` before unlocking it
        let clock = info.clock;
        info.blocked_by = blocked_by.clone();
        let blocked_by_len = blocked_by.len();
        drop(info);
        drop(info_ref);

        // decision tracks what we should do in the end, after iterating each
        // of the commands that is blocking us
        #[derive(PartialEq, Eq, Debug)]
        enum Reply {
            ACCEPT,
            REJECT,
            WAIT,
        }
        let mut reply = Reply::WAIT;
        let mut not_blocked_by = HashSet::new();

        // we send an ok if no command is blocking this command
        let ok = blocked_by.is_empty();

        if ok {
            reply = Reply::ACCEPT;
        } else if !self.wait_condition {
            // if the wait condition is not enabled, reject right away
            reply = Reply::REJECT;
        } else {
            // if there are commands blocking us, iterate each of them and check
            // if they are still blocking us (in the meantime, then may have
            // been moved to the `ACCEPT` or `COMMIT` phase, and in that case we
            // might be able to ignore them)
            trace!(
                "p{}: MPropose({:?}) blocked by {:?} | time={}",
                self.id(),
                dot,
                blocked_by,
                time.micros()
            );
            for blocked_by_dot in blocked_by {
                if let Some(blocked_by_dot_ref) = self.cmds.get(blocked_by_dot)
                {
                    // in this case, this the command hasn't been GCed since we
                    // got it from the key clocks, so we need to consider it
                    let blocked_by_info = blocked_by_dot_ref.read();

                    // check whether this command has already a "good enough"
                    // clock and dep
                    let has_clock_and_dep = matches!(
                        blocked_by_info.status,
                        Status::ACCEPT | Status::COMMIT
                    );
                    if has_clock_and_dep {
                        // if the clock and dep are "good enough", check if we
                        // can ignore the command
                        let safe_to_ignore = Self::safe_to_ignore(
                            self.bp.process_id,
                            dot,
                            clock,
                            blocked_by_info.clock,
                            &blocked_by_info.deps,
                            time,
                        );
                        trace!(
                            "p{}: MPropose({:?}) safe to ignore {:?}: {:?} | time={}",
                            self.bp.process_id,
                            dot,
                            blocked_by_dot,
                            safe_to_ignore,
                            time.micros()
                        );
                        if safe_to_ignore {
                            // the command can be ignored, and so we register
                            // that this command is in fact not blocking our
                            // command
                            not_blocked_by.insert(blocked_by_dot);
                        } else {
                            // if there's a single command that can't be
                            // ignored, our command must be rejected, and so we
                            // `break` as there's no point in checking all the
                            // other commands
                            reply = Reply::REJECT;
                            break;
                        }
                    } else {
                        // if the clock and dep are not "good enough", we're
                        // blocked by this command
                        trace!(
                            "p{}: MPropose({:?}) still blocked by {:?} | time={}",
                            self.bp.process_id,
                            dot,
                            blocked_by_dot,
                            time.micros()
                        );
                        // upgrade lock guard to a mutable one
                        drop(blocked_by_info);
                        let mut blocked_by_info = blocked_by_dot_ref.write();
                        // register that this command is blocking our command
                        blocked_by_info.blocking.insert(dot);
                    }
                } else {
                    trace!(
                        "p{}: MPropose({:?}) no longer blocked by {:?} | time={}",
                        self.bp.process_id,
                        dot,
                        blocked_by_dot,
                        time.micros()
                    );
                    // in this case, the command has been GCed, and for that
                    // reason we simply record that it can be ignored
                    // (as it has already been executed at all processes)
                    not_blocked_by.insert(blocked_by_dot);
                }
            }

            if not_blocked_by.len() == blocked_by_len {
                // if in the end it turns out that we're not blocked by any
                // command, accept this command:
                // - in this case, we must still have `Reply::WAIT`, or in other
                //   words, it can't be `Reply::REJECT`
                assert_eq!(reply, Reply::WAIT);
                reply = Reply::ACCEPT;
            }
        };

        trace!(
            "p{}: MPropose({:?}) decision {:?} | time={}",
            self.bp.process_id,
            dot,
            reply,
            time.micros()
        );

        // it's not possible that the command was GCed; for that, we would need
        // to have executed it, but that's just not possible, as only this
        // workers handles messages about this command; for this reason, we can
        // have the `expect` below
        let info_ref = self
            .cmds
            .get(dot)
            .expect("the command must not have been GCed in the meantime");
        let mut info = info_ref.write();

        // for the same reason as above, the command phase must still be
        // `Status::PROPOSE`
        assert_eq!(info.status, Status::PROPOSE);

        match reply {
            Reply::ACCEPT => Self::accept_command(
                self.bp.process_id,
                dot,
                &mut info,
                &mut self.to_processes,
                time,
            ),
            Reply::REJECT => Self::reject_command(
                self.bp.process_id,
                dot,
                &mut info,
                &mut self.key_clocks,
                &mut self.to_processes,
                time,
            ),
            Reply::WAIT => {
                // in this case, we simply update the set of commands we need to
                // wait for (since we may have decided to ignore some above)
                for not_blocked_by_dot in not_blocked_by {
                    info.blocked_by.remove(&not_blocked_by_dot);
                }
                // after this, we must still be blocked by some command
                assert!(!info.blocked_by.is_empty());

                // save the current time as the moment where we started waiting
                info.wait_start_time_ms = Some(time.millis());
            }
        }

        drop(info);
        drop(info_ref);

        // check if there's a buffered retry request; if yes, handle the retry
        // again (since now we have the payload)
        if let Some((from, clock, deps)) = self.buffered_retries.remove(&dot) {
            self.handle_mretry(from, dot, clock, deps, time);
        }

        // check if there's a buffered commit notification; if yes, handle the
        // commit again (since now we have the payload)
        if let Some((from, clock, deps)) = self.buffered_commits.remove(&dot) {
            self.handle_mcommit(from, dot, clock, deps, time);
        }
    }

    fn handle_mproposeack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MProposeAck({:?}, {:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            deps,
            ok,
            from,
            _time.micros()
        );

        // get cmd info
        let info_ref = self.cmds.get_or_default(dot);
        let mut info = info_ref.write();

        // do nothing if we're no longer PROPOSE or REJECT (yes, it seems that
        // the coordinator can reject its own command; this case was only
        // occurring in the simulator, but with concurrency I think it can
        // happen in the runner as well, as it will be tricky to ensure a level
        // of atomicity where the coordinator never rejects its own command):
        // - this ensures that once an MCommit/MRetry is sent in this handler,
        //   further messages received are ignored
        // - we can check this by asserting that `info.quorum_clocks.all()` is
        //   false, before adding any new info, as we do below
        if !matches!(info.status, Status::PROPOSE | Status::REJECT) {
            return;
        }
        if info.quorum_clocks.all() {
            panic!(
                "p{}: {:?} already had all MProposeAck needed",
                self.bp.process_id, dot
            );
        }

        // update quorum deps
        info.quorum_clocks.add(from, clock, deps, ok);

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // if yes, get the aggregated results
            let (aggregated_clock, aggregated_deps, aggregated_ok) =
                info.quorum_clocks.aggregated();

            // fast path condition: all processes reported ok
            if aggregated_ok {
                // in this case, all processes have accepted the proposal by the
                // coordinator; check that that's the case
                assert_eq!(aggregated_clock, info.clock);

                self.bp.fast_path();
                // fast path: create `MCommit`
                let mcommit = Message::MCommit {
                    dot,
                    clock: aggregated_clock,
                    deps: aggregated_deps,
                };
                let target = self.bp.all();

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mcommit,
                });
            } else {
                self.bp.slow_path();
                // slow path: create `MRetry`
                let mconsensus = Message::MRetry {
                    dot,
                    clock: aggregated_clock,
                    deps: aggregated_deps,
                };
                // here we send to everyone because this message may unblock
                // blocked commads; by only sending it to a majority, we would
                // potentially block commands unnecessarily
                let target = self.bp.all();

                // save new action
                self.to_processes.push(Action::ToSend {
                    target,
                    msg: mconsensus,
                });
            }
        }
    }

    fn handle_mcommit(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MCommit({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            deps,
            from,
            time.micros()
        );

        // merge clocks
        self.key_clocks.clock_join(&clock);

        // get cmd info
        let info_ref = self.cmds.get_or_default(dot);
        let mut info = info_ref.write();

        if info.status == Status::START {
            // save this notification just in case we've received the `MPropose`
            // and `MCommit` in opposite orders (due to multiplexing)
            self.buffered_commits.insert(dot, (from, clock, deps));
            return;
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return;
        }

        // register commit time if we're the coordinator
        if dot.source() == from {
            let start_time_ms = info.start_time_ms.take().expect(
                "the command should have been started by its coordinator",
            );
            let end_time_ms = time.millis();

            // compute commit latency and collect this metric
            let commit_latency = end_time_ms - start_time_ms;
            self.bp.collect_metric(
                ProtocolMetricsKind::CommitLatency,
                commit_latency,
            );
        }

        // create execution info
        let cmd = info.cmd.clone().expect("there should be a command payload");
        let execution_info = ExecutionInfo::new(dot, cmd, clock, deps.clone());
        self.to_executors.push(execution_info);

        // update command info:
        info.status = Status::COMMIT;
        info.deps = deps.clone();
        Self::update_clock(&mut self.key_clocks, dot, &mut info, clock);

        // take the set of commands that this command is blocking and try to
        // unblock them
        let blocking = std::mem::take(&mut info.blocking);
        drop(info);
        drop(info_ref);
        self.try_to_unblock(dot, clock, deps, blocking, time);

        // if we're not running gc, remove the dot info now
        if !self.gc_running() {
            self.gc_command(dot);
        }
    }

    fn handle_mretry(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MRetry({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            clock,
            deps,
            from,
            time.micros()
        );

        // merge clocks
        self.key_clocks.clock_join(&clock);

        // get cmd info
        let info_ref = self.cmds.get_or_default(dot);
        let mut info = info_ref.write();

        if info.status == Status::START {
            // save this notification just in case we've received the `MPropose`
            // and `MRetry` in opposite orders (due to multiplexing)
            self.buffered_retries.insert(dot, (from, clock, deps));
            return;
        }

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            return;
        }

        // update command info:
        info.status = Status::ACCEPT;
        info.deps = deps.clone();
        Self::update_clock(&mut self.key_clocks, dot, &mut info, clock);

        // compute new set of predecessors for the command
        let cmd = info.cmd.as_ref().expect("command has been set");
        let blocking = None;
        let mut new_deps =
            self.key_clocks.predecessors(dot, cmd, clock, blocking);

        // aggregate with incoming deps
        new_deps.extend(deps.clone());

        // create message and target
        let msg = Message::MRetryAck {
            dot,
            deps: new_deps,
        };
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend { target, msg });

        // take the set of commands that this command is blocking and try to
        // unblock them
        let blocking = std::mem::take(&mut info.blocking);
        drop(info);
        drop(info_ref);
        self.try_to_unblock(dot, clock, deps, blocking, time);
    }

    fn handle_mretryack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        deps: HashSet<Dot>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MRetryAck({:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            deps,
            from,
            _time.micros()
        );

        // get cmd info
        let info_ref = self.cmds.get_or_default(dot);
        let mut info = info_ref.write();

        // do nothing if we're no longer ACCEPT:
        // - this ensures that once an MCommit is sent in this handler, further
        //   messages received are ignored
        // - we can check this by asserting that `info.quorum_retries.all()` is
        //   false, before adding any new info, as we do below
        if info.status != Status::ACCEPT {
            return;
        }
        if info.quorum_retries.all() {
            panic!(
                "p{}: {:?} already had all MRetryAck needed",
                self.bp.process_id, dot
            );
        }

        // update quorum retries
        info.quorum_retries.add(from, deps);

        // check if we have all necessary replies
        if info.quorum_retries.all() {
            // if yes, get the aggregated results
            let aggregated_deps = info.quorum_retries.aggregated();

            // create message and target
            let mcommit = Message::MCommit {
                dot,
                clock: info.clock,
                deps: aggregated_deps,
            };
            let target = self.bp.all();

            // save new action
            self.to_processes.push(Action::ToSend {
                target,
                msg: mcommit,
            });
        }
    }

    fn handle_mgc(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MGarbageCollection({:?}) from {} | time={}",
            self.id(),
            committed,
            from,
            _time.micros()
        );
        self.gc_track.update_clock_of(from, committed);

        // compute newly stable dots
        let stable = self.gc_track.stable();

        // since the dot info is shared across workers, we don't need to send
        // an MStable message to all the workers, as in the other protocols,
        // we can do it right here
        let dots: Vec<_> = util::dots(stable).collect();
        self.bp.stable(dots.len());
        dots.into_iter().for_each(|dot| self.gc_command(dot));
    }

    fn handle_event_garbage_collection(&mut self, _time: &dyn SysTime) {
        trace!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.micros()
        );

        // retrieve the committed clock
        let committed = self.gc_track.clock();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }

    fn update_clock(
        key_clocks: &mut KC,
        dot: Dot,
        info: &mut RwLockWriteGuard<'_, CaesarInfo>,
        new_clock: Clock,
    ) {
        // get the command
        let cmd = info.cmd.as_ref().expect("command has been set");

        // remove previous clock (if any)
        Self::remove_clock(key_clocks, cmd, info.clock);

        // add new clock to key clocks
        key_clocks.add(dot, &cmd, new_clock);

        // finally update the clock
        info.clock = new_clock;
    }

    fn remove_clock(key_clocks: &mut KC, cmd: &Command, clock: Clock) {
        // remove previous clock from key clocks if we added it before
        let added_before = !clock.is_zero();
        if added_before {
            key_clocks.remove(cmd, clock);
        }
    }

    fn gc_command(&mut self, dot: Dot) {
        if let Some(info) = self.cmds.gc_single(&dot) {
            // get the command
            let cmd = info.cmd.as_ref().expect("command has been set");

            // remove previous clock (if any)
            // TODO: we're gcing a command from the key clocks when it's
            // committed at all processes but this may not be safe; I'm thinking
            // that it should be when it's executed at all processes?
            Self::remove_clock(&mut self.key_clocks, cmd, info.clock);
        } else {
            panic!("we're the single worker performing gc, so all commands should exist");
        }
    }

    fn safe_to_ignore(
        _id: ProcessId,
        my_dot: Dot,
        my_clock: Clock,
        their_clock: Clock,
        their_deps: &HashSet<Dot>,
        _time: &dyn SysTime,
    ) -> bool {
        trace!(
            "p{}: safe_to_ignore({:?}, {:?}, {:?}, {:?}) | time={}",
            _id,
            my_dot,
            my_clock,
            their_clock,
            their_deps,
            _time.micros()
        );
        // since clocks can only increase, the clock of the blocking command
        // must be higher than ours (otherwise it couldn't have been
        // reported as blocking in the first place)
        assert!(my_clock < their_clock);
        // since we (currently) have a lower clock than the command blocking us,
        // it is only safe to ignore it if we are included in its dependencies
        their_deps.contains(&my_dot)
    }

    fn try_to_unblock(
        &mut self,
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        blocking: HashSet<Dot>,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: try_to_unblock({:?}, {:?}, {:?}, {:?}) | time={}",
            self.id(),
            dot,
            clock,
            deps,
            blocking,
            time.micros()
        );

        for blocked_dot in blocking {
            trace!(
                "p{}: try_to_unblock({:?}) checking {:?} | time={}",
                self.bp.process_id,
                dot,
                blocked_dot,
                time.micros()
            );

            if let Some(blocked_dot_info_ref) = self.cmds.get(blocked_dot) {
                let mut blocked_dot_info = blocked_dot_info_ref.write();

                // we only need to accept/reject the blocked command if the
                // command is still at the `PROPOSE` phase
                if blocked_dot_info.status == Status::PROPOSE {
                    let mut end_of_wait = false;
                    let safe_to_ignore = Self::safe_to_ignore(
                        self.bp.process_id,
                        blocked_dot,
                        blocked_dot_info.clock,
                        clock,
                        &deps,
                        time,
                    );
                    if safe_to_ignore {
                        // if it's safe to ignore the command, remove it from
                        // the set of commands that are blocking this blocked
                        // command
                        blocked_dot_info.blocked_by.remove(&dot);

                        if blocked_dot_info.blocked_by.is_empty() {
                            // ACCEPT the blocked command if it no longer has
                            // commands blocking it
                            Self::accept_command(
                                self.bp.process_id,
                                blocked_dot,
                                &mut blocked_dot_info,
                                &mut self.to_processes,
                                time,
                            );
                            // we're done waiting
                            end_of_wait = true;
                        }
                    } else {
                        // REJECT the blocked command if it's not safe to ignore
                        // this command; this means that we reject the command
                        // ASAP (i.e. we don't wait for all commands that are
                        // blocking us)
                        Self::reject_command(
                            self.bp.process_id,
                            blocked_dot,
                            &mut blocked_dot_info,
                            &mut self.key_clocks,
                            &mut self.to_processes,
                            time,
                        );
                        // we're done waiting
                        end_of_wait = true;
                    }

                    if end_of_wait {
                        // get wait start and end time
                        let wait_start_time_ms =
                            blocked_dot_info.wait_start_time_ms.take().expect(
                                "a blocked command must have a wait start time",
                            );
                        let wait_end_time_ms = time.millis();

                        // compute wait condition delay and collect this metric
                        let wait_delay = wait_end_time_ms - wait_start_time_ms;
                        self.bp.collect_metric(
                            ProtocolMetricsKind::WaitConditionDelay,
                            wait_delay,
                        );
                    }
                } else {
                    trace!(
                        "p{}: try_to_unblock({:?}) {:?} no longer at PROPOSE | time={}",
                        self.bp.process_id,
                        dot,
                        blocked_dot,
                        time.micros()
                    );
                }
            } else {
                trace!(
                    "p{}: try_to_unblock({:?}) {:?} already GCed | time={}",
                    self.bp.process_id,
                    dot,
                    blocked_dot,
                    time.micros()
                );
            }
        }
    }

    fn accept_command(
        _id: ProcessId,
        dot: Dot,
        info: &mut RwLockWriteGuard<'_, CaesarInfo>,
        to_processes: &mut Vec<Action<Self>>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: accept_command({:?}) with {:?} {:?}| time={}",
            _id,
            dot,
            info.clock,
            info.deps,
            _time.micros()
        );
        Self::send_mpropose_ack(
            dot,
            info.clock,
            info.deps.clone(),
            true,
            to_processes,
        )
    }

    fn reject_command(
        _id: ProcessId,
        dot: Dot,
        info: &mut RwLockWriteGuard<'_, CaesarInfo>,
        key_clocks: &mut KC,
        to_processes: &mut Vec<Action<Self>>,
        _time: &dyn SysTime,
    ) {
        // if not ok, reject the coordinator's timestamp
        info.status = Status::REJECT;

        // compute new timestamp for the command
        let new_clock = key_clocks.clock_next();

        // compute new set of predecessors for the command
        let cmd = info.cmd.as_ref().expect("command has been set");
        let blocking = None;
        let new_deps = key_clocks.predecessors(dot, cmd, new_clock, blocking);

        trace!(
            "p{}: reject_command({:?}) with {:?} {:?}| time={}",
            _id,
            dot,
            new_clock,
            new_deps,
            _time.micros()
        );
        Self::send_mpropose_ack(dot, new_clock, new_deps, false, to_processes);
    }

    // helper to send an `MProposeAck`
    fn send_mpropose_ack(
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
        to_processes: &mut Vec<Action<Self>>,
    ) {
        // create `MProposeAck` and target
        let mproposeack = Message::MProposeAck {
            dot,
            clock,
            deps,
            ok,
        };
        let from = dot.source();
        let target = singleton![from];

        // save new action
        to_processes.push(Action::ToSend {
            target,
            msg: mproposeack,
        });
    }
}

// `CaesarInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone, PartialEq, Eq)]
struct CaesarInfo {
    status: Status,
    // `None` if not set yet
    cmd: Option<Command>,
    clock: Clock,
    deps: HashSet<Dot>,
    // set of commands that this command is blocking
    blocking: HashSet<Dot>,
    // set of commands that this command is blocked by
    blocked_by: HashSet<Dot>,
    // `quorum_clocks` is used by the coordinator to aggregate fast-quorum
    // replies and make the fast-path decision
    quorum_clocks: QuorumClocks,
    // `quorum_retries` is used by the coordinator to aggregate dependencies
    // reported in `MRetry` messages
    quorum_retries: QuorumRetries,
    // time in milliseconds when the coordinator received the command
    start_time_ms: Option<u64>,
    // time in milliseconds when this process decided to start the wait
    // condition
    wait_start_time_ms: Option<u64>,
}

impl Info for CaesarInfo {
    fn new(
        process_id: ProcessId,
        _shard_id: ShardId,
        _n: usize,
        _f: usize,
        fast_quorum_size: usize,
        write_quorum_size: usize,
    ) -> Self {
        Self {
            status: Status::START,
            cmd: None,
            clock: Clock::new(process_id),
            deps: HashSet::new(),
            blocking: HashSet::new(),
            blocked_by: HashSet::new(),
            quorum_clocks: QuorumClocks::new(
                process_id,
                fast_quorum_size,
                write_quorum_size,
            ),
            quorum_retries: QuorumRetries::new(write_quorum_size),
            start_time_ms: None,
            wait_start_time_ms: None,
        }
    }
}

// `Caesar` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    MPropose {
        dot: Dot,
        cmd: Command,
        clock: Clock,
    },
    MProposeAck {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
        ok: bool,
    },
    MCommit {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
    },
    MRetry {
        dot: Dot,
        clock: Clock,
        deps: HashSet<Dot>,
    },
    MRetryAck {
        dot: Dot,
        deps: HashSet<Dot>,
    },
    MGarbageCollection {
        committed: VClock<ProcessId>,
    },
}

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::run::{
            worker_dot_index_shift, worker_index_no_shift, GC_WORKER_INDEX,
        };
        // TODO: the dot info is shared across workers, and in this case we can
        // select a random worker, not a selection based on the dot; do we want
        // to do that?
        // - maybe no; if we keep the current indexing we can at least be sure
        //   that there won't be any other worker processing messages about the
        //   same command concurrently (e.g. two MProposeAcks received at the
        //   same time that are handled by different workers)
        // - well, maybe the above is fine, since we're locking the command, but
        //   maybe it's not good for performance
        match self {
            // Protocol messages
            Self::MPropose { dot, .. } => worker_dot_index_shift(&dot),
            Self::MProposeAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MRetry { dot, .. } => worker_dot_index_shift(&dot),
            Self::MRetryAck { dot, .. } => worker_dot_index_shift(&dot),
            // GC messages
            Self::MGarbageCollection { .. } => {
                worker_index_no_shift(GC_WORKER_INDEX)
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
        use fantoch::run::{worker_index_no_shift, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    START,
    PROPOSE,
    REJECT,
    ACCEPT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, KeyGen, Workload};
    use fantoch::planet::{Planet, Region};
    use fantoch::sim::Simulation;
    use fantoch::time::SimTime;

    #[test]
    fn sequential_caesar_test() {
        caesar_flow::<SequentialKeyClocks>();
    }

    #[test]
    fn locked_caesar_test() {
        caesar_flow::<LockedKeyClocks>();
    }

    fn caesar_flow<KD: KeyClocks>() {
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
        let executor_1 =
            PredecessorsExecutor::new(process_id_1, shard_id, config);
        let executor_2 =
            PredecessorsExecutor::new(process_id_2, shard_id, config);
        let executor_3 =
            PredecessorsExecutor::new(process_id_3, shard_id, config);

        // caesar
        let (mut caesar_1, _) =
            Caesar::<KD>::new(process_id_1, shard_id, config);
        let (mut caesar_2, _) =
            Caesar::<KD>::new(process_id_2, shard_id, config);
        let (mut caesar_3, _) =
            Caesar::<KD>::new(process_id_3, shard_id, config);

        // discover processes in all caesar
        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );
        caesar_1.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &europe_west3,
            &planet,
            processes.clone(),
        );
        caesar_2.discover(sorted);
        let sorted = util::sort_processes_by_distance(
            &us_west1,
            &planet,
            processes.clone(),
        );
        caesar_3.discover(sorted);

        // register processes
        simulation.register_process(caesar_1, executor_1);
        simulation.register_process(caesar_2, executor_2);
        simulation.register_process(caesar_3, executor_3);

        // client workload
        let shard_count = 1;
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 100,
            pool_size: 1,
        };
        let keys_per_command = 1;
        let commands_per_client = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // create client 1 that is connected to caesar 1
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

        // check that `target` is caesar 1
        assert_eq!(target, process_id_1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in caesar 1
        let (process, _, pending, time) = simulation.get_process(target);
        pending.wait_for(&cmd);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mpropose = actions.pop().unwrap();

        // check that the mpropose is being sent to *all* processes
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mpropose.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle mproposes
        let mut mproposeacks =
            simulation.forward_to_processes((process_id_1, mpropose));

        // check that there are 3 mproposeacks
        assert_eq!(mproposeacks.len(), 3);

        // handle the first mproposeack
        let mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mproposeack
        let mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
        );
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the third mproposeack
        let mut mcommits = simulation.forward_to_processes(
            mproposeacks.pop().expect("there should be an mpropose ack"),
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

        // there should be no new sends
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
        let mpropose = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MPropose {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(
            matches!(mpropose, Action::ToSend {msg, ..} if check_msg(&msg))
        );
    }

    #[test]
    fn caesar_livelock() {
        // there's a single shard
        let shard_id = 0;

        // processes
        let processes = vec![(1, shard_id), (2, shard_id), (3, shard_id)];

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // caesar
        let (mut caesar_1, _) = CaesarSequential::new(1, shard_id, config);
        let (mut caesar_2, _) = CaesarSequential::new(2, shard_id, config);
        let (mut caesar_3, _) = CaesarSequential::new(3, shard_id, config);

        // discover processes in all caesar (the order doesn't matter)
        caesar_1.discover(processes.clone());
        caesar_2.discover(processes.clone());
        caesar_3.discover(processes.clone());

        // client workload: all commands conflict
        let shard_count = 1;
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 100,
            pool_size: 1,
        };
        let keys_per_command = 1;
        let commands_per_client = 10;
        let payload_size = 0;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // create client that will send all commands
        let mut client = Client::new(1, workload, None);

        // generate a new command by the client
        let mut next_cmd = || {
            let (_, cmd) = client
                .next_cmd(&time)
                .expect("there should be a next command");
            cmd
        };

        // retrieve a single outgoing message from process
        let retrieve_single_msg = |caesar: &mut CaesarSequential| {
            let mut actions: Vec<_> = caesar.to_processes_iter().collect();
            assert_eq!(actions.len(), 1);
            let action = actions.pop().unwrap();
            match action {
                Action::ToSend { msg, .. } => msg,
                _ => panic!("expecting Action::ToSend"),
            }
        };

        // submit a command, take the mpropose, handle it locally and return it
        let mut submit = |caesar: &mut CaesarSequential| {
            caesar.submit(None, next_cmd(), &time);
            let mpropose = retrieve_single_msg(caesar);

            // handle the mpropose locally and ignore the mpropose ack
            caesar.handle(caesar.id(), shard_id, mpropose.clone(), &time);
            let _mpropose_ack = retrieve_single_msg(caesar);

            // return the mpropose
            mpropose
        };

        let handle = |caesar: &mut CaesarSequential, from, msg| {
            caesar.handle(from, shard_id, msg, &time);
            let actions: Vec<_> = caesar.to_processes_iter().collect();
            assert!(actions.is_empty());
        };

        // submit two commands at process 1 and one command at the other two
        let mpropose_1 = submit(&mut caesar_1);
        let mpropose_2 = submit(&mut caesar_2);
        let mpropose_3 = submit(&mut caesar_3);
        let mpropose_4 = submit(&mut caesar_1);

        // handle:
        // - mpropose_1 at process 2
        // - mpropose_2 at process 3
        // - mpropose_3 at process 1
        // and check that no new message is produced (i.e. the commands are
        // blocked in the wait condition)
        handle(&mut caesar_2, 1, mpropose_1);
        handle(&mut caesar_3, 2, mpropose_2);
        handle(&mut caesar_1, 3, mpropose_3);

        // submit one command at each process
        let mpropose_5 = submit(&mut caesar_2);
        let mpropose_6 = submit(&mut caesar_3);
        let mpropose_7 = submit(&mut caesar_1);

        // handle:
        // - mpropose_4 at process 2
        // - mpropose_5 at process 3
        // - mpropose_6 at process 1
        // and check that no new message is produced (i.e. the commands are
        // blocked in the wait condition)
        handle(&mut caesar_2, 1, mpropose_4);
        handle(&mut caesar_3, 2, mpropose_5);
        handle(&mut caesar_1, 3, mpropose_6);

        // submit one command at each process
        let mpropose_8 = submit(&mut caesar_2);
        let mpropose_9 = submit(&mut caesar_3);
        // this sequence could continue here, but let's stop
        let _mpropose_10 = submit(&mut caesar_1);

        // handle:
        // - mpropose_7 at process 2
        // - mpropose_8 at process 3
        // - mpropose_9 at process 1
        // and check that no new message is produced (i.e. the commands are
        // blocked in the wait condition)
        handle(&mut caesar_2, 1, mpropose_7);
        handle(&mut caesar_3, 2, mpropose_8);
        handle(&mut caesar_1, 3, mpropose_9);
    }
}
