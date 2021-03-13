use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult, DEFAULT_SHARD_ID};
use crate::config::Config;
use crate::executor::{ExecutionOrderMonitor, Executor, ExecutorMetrics};
use crate::id::{ClientId, ProcessId, ShardId};
use crate::metrics::Histogram;
use crate::planet::{Planet, Region};
use crate::protocol::{Action, Protocol, ProtocolMetrics};
use crate::sim::{Schedule, Simulation};
use crate::time::SysTime;
use crate::util;
use crate::HashMap;
use rand::Rng;
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;

#[derive(PartialEq, Eq)]
enum ScheduleAction<Message, PeriodicEvent> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, ShardId, ProcessId, Message),
    SendToClient(ClientId, CommandResult),
    PeriodicProcessEvent(ProcessId, PeriodicEvent, Duration),
    PeriodicExecutedNotification(ProcessId, Duration),
}
#[derive(Clone)]
enum MessageRegion {
    Process(ProcessId),
    Client(ClientId),
}

pub struct Runner<P: Protocol> {
    planet: Planet,
    simulation: Simulation<P>,
    schedule: Schedule<ScheduleAction<P::Message, P::PeriodicEvent>>,
    // mapping from process identifier to its region
    process_to_region: HashMap<ProcessId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
    // total number of clients
    client_count: usize,
    // boolean indicating whether the runner should make the distance between
    // regions symmetric
    make_distances_symmetric: bool,
    // boolean indicating whether the runner should reoder messages
    reorder_messages: bool,
}

#[derive(PartialEq)]
enum SimulationStatus {
    ClientsRunning,
    ExtraSimulationTime,
    Done,
}

impl<P> Runner<P>
where
    P: Protocol,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of
    /// regions:
    /// - `process_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new(
        planet: Planet,
        config: Config,
        workload: Workload,
        clients_per_process: usize,
        process_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self {
        // check that we have the correct number of `process_regions`
        assert_eq!(process_regions.len(), config.n());
        assert!(config.gc_interval().is_some());

        // create simulation
        let mut simulation = Simulation::new();

        // create processes
        let mut processes = Vec::with_capacity(config.n());
        let mut periodic_process_events = Vec::new();
        let mut periodic_executed_notifications = Vec::new();

        // there's a single shard
        let shard_id = 0;

        let to_discover: Vec<_> = process_regions
            .into_iter()
            .zip(util::process_ids(shard_id, config.n()))
            .map(|(region, process_id)| {
                // create process and save it
                let (process, process_events) =
                    P::new(process_id, shard_id, config);
                processes.push((region.clone(), process));

                // save periodic process events
                periodic_process_events.extend(
                    process_events
                        .into_iter()
                        .map(|(event, delay)| (process_id, event, delay)),
                );

                // save periodic executed notifications
                let executed_notification_interval =
                    config.executor_executed_notification_interval();
                periodic_executed_notifications
                    .push((process_id, executed_notification_interval));

                (process_id, shard_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover
            .clone()
            .into_iter()
            .map(|(process_id, _, region)| (process_id, region))
            .collect();

        // register processes
        processes.into_iter().for_each(|(region, mut process)| {
            // discover
            let sorted = util::sort_processes_by_distance(
                &region,
                &planet,
                to_discover.clone(),
            );
            let (connect_ok, _) = process.discover(sorted);
            assert!(connect_ok);

            // create executor for this process
            let executor = <P::Executor as Executor>::new(
                process.id(),
                process.shard_id(),
                config,
            );

            // and register both
            simulation.register_process(process, executor);
        });

        // register clients and create client to region mapping
        let mut client_id = 0;
        let mut client_to_region = HashMap::new();
        for region in client_regions {
            for _ in 1..=clients_per_process {
                // create client
                client_id += 1;
                let status_frequency = None;
                let mut client =
                    Client::new(client_id, workload, status_frequency);
                // discover
                let closest = util::closest_process_per_shard(
                    &region,
                    &planet,
                    to_discover.clone(),
                );
                client.connect(closest);
                // and register it
                simulation.register_client(client);
                client_to_region.insert(client_id, region.clone());
            }
        }

        // create runner
        let mut runner = Self {
            planet,
            simulation,
            schedule: Schedule::new(),
            process_to_region,
            client_to_region,
            // since we start ids in 1, the last id is the same as the number of
            // clients
            client_count: client_id as usize,
            make_distances_symmetric: false,
            reorder_messages: false,
        };

        // schedule periodic process events
        for (process_id, event, delay) in periodic_process_events {
            runner.schedule_periodic_process_event(process_id, event, delay);
        }

        // schedule periodic executed notifications
        for (process_id, delay) in periodic_executed_notifications {
            runner.schedule_periodic_executed_notification(process_id, delay)
        }

        runner
    }

    pub fn make_distances_symmetric(&mut self) {
        self.make_distances_symmetric = true;
    }

    pub fn reorder_messages(&mut self) {
        self.reorder_messages = true;
    }

    /// Run the simulation. `extra_sim_time` indicates how much longer should
    /// the simulation run after clients are finished.
    pub fn run(
        &mut self,
        extra_sim_time: Option<Duration>,
    ) -> (
        HashMap<ProcessId, (ProtocolMetrics, ExecutorMetrics)>,
        HashMap<ProcessId, Option<ExecutionOrderMonitor>>,
        HashMap<Region, (usize, Histogram)>,
    ) {
        // start clients
        self.simulation.start_clients().into_iter().for_each(
            |(client_id, process_id, cmd)| {
                // schedule client commands
                self.schedule_submit(
                    MessageRegion::Client(client_id),
                    process_id,
                    cmd,
                )
            },
        );

        // run simulation loop
        self.simulation_loop(extra_sim_time);

        // return metrics and client latencies
        (
            self.metrics(),
            self.executors_monitors(),
            self.clients_latencies(),
        )
    }

    fn simulation_loop(&mut self, extra_sim_time: Option<Duration>) {
        let mut simulation_status = SimulationStatus::ClientsRunning;
        let mut clients_done = 0;
        let mut simulation_final_time = 0;

        while simulation_status != SimulationStatus::Done {
            let action = self.schedule
                .next_action(self.simulation.time())
                .expect("there should be a new action since stability is always running");

            match action {
                ScheduleAction::PeriodicProcessEvent(
                    process_id,
                    event,
                    delay,
                ) => {
                    self.handle_periodic_process_event(process_id, event, delay)
                }
                ScheduleAction::PeriodicExecutedNotification(
                    process_id,
                    delay,
                ) => self
                    .handle_periodic_executed_notification(process_id, delay),
                ScheduleAction::SubmitToProc(process_id, cmd) => {
                    self.handle_submit_to_proc(process_id, cmd);
                }
                ScheduleAction::SendToProc(
                    from,
                    from_shard_id,
                    process_id,
                    msg,
                ) => {
                    self.handle_send_to_proc(
                        from,
                        from_shard_id,
                        process_id,
                        msg,
                    );
                }
                ScheduleAction::SendToClient(client_id, cmd_result) => {
                    // handle new command result in client
                    let submit = self.simulation.forward_to_client(cmd_result);
                    if let Some((process_id, cmd)) = submit {
                        self.schedule_submit(
                            MessageRegion::Client(client_id),
                            process_id,
                            cmd,
                        );
                    } else {
                        clients_done += 1;
                        // if all clients are done, enter the next phase
                        if clients_done == self.client_count {
                            simulation_status = match extra_sim_time {
                                Some(extra) => {
                                    // if there's extra time, compute the
                                    // final simulation time
                                    simulation_final_time =
                                        self.simulation.time().millis()
                                            + extra.as_millis() as u64;
                                    SimulationStatus::ExtraSimulationTime
                                }
                                None => {
                                    // otherwise, end the simulation
                                    SimulationStatus::Done
                                }
                            }
                        }
                    }
                }
            }

            // check if we're in extra simulation time; if yes, finish the
            // simulation if we're past the final simulation time
            let should_end_sim = simulation_status
                == SimulationStatus::ExtraSimulationTime
                && self.simulation.time().millis() > simulation_final_time;
            if should_end_sim {
                simulation_status = SimulationStatus::Done;
            }
        }
    }

    fn handle_periodic_process_event(
        &mut self,
        process_id: ProcessId,
        event: P::PeriodicEvent,
        delay: Duration,
    ) {
        // get process
        let (process, _, _, time) = self.simulation.get_process(process_id);

        // handle event adn schedule new actions
        process.handle_event(event.clone(), time);
        self.send_to_processes_and_executors(process_id);

        // schedule the next periodic event
        self.schedule_periodic_process_event(process_id, event, delay);
    }

    fn handle_periodic_executed_notification(
        &mut self,
        process_id: ProcessId,
        delay: Duration,
    ) {
        // get process and executor
        let (process, executor, _, time) =
            self.simulation.get_process(process_id);

        // handle executed and schedule new actions
        if let Some((committed, executed)) = executor.committed_and_executed(time) {
            process.handle_committed_and_executed(committed, executed, time);
            self.send_to_processes_and_executors(process_id);
        }

        // schedule the next periodic event
        self.schedule_periodic_executed_notification(process_id, delay);
    }

    fn handle_submit_to_proc(&mut self, process_id: ProcessId, cmd: Command) {
        // get process and executor
        let (process, _executor, pending, time) =
            self.simulation.get_process(process_id);

        // register command in pending
        pending.wait_for(&cmd);

        // submit to process and schedule new actions
        process.submit(None, cmd, time);
        self.send_to_processes_and_executors(process_id);
    }

    fn handle_send_to_proc(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        process_id: ProcessId,
        msg: P::Message,
    ) {
        // get process and executor
        let (process, _, _, time) = self.simulation.get_process(process_id);

        // handle message and schedule new actions
        process.handle(from, from_shard_id, msg, time);
        self.send_to_processes_and_executors(process_id);
    }

    // (maybe) Schedules a new submit from a client.
    fn schedule_submit(
        &mut self,
        from_region: MessageRegion,
        process_id: ProcessId,
        cmd: Command,
    ) {
        // create action and schedule it
        let action = ScheduleAction::SubmitToProc(process_id, cmd);
        self.schedule_message(
            from_region,
            MessageRegion::Process(process_id),
            action,
        );
    }

    fn send_to_processes_and_executors(&mut self, process_id: ProcessId) {
        // get process and executor
        let (process, executor, pending, time) =
            self.simulation.get_process(process_id);
        assert_eq!(process.id(), process_id);
        let shard_id = process.shard_id();

        // get ready commands
        let protocol_actions = process.to_processes_iter().collect();

        // handle new execution info in the executor
        let ready: Vec<_> = process
            .to_executors_iter()
            .flat_map(|info| {
                executor.handle(info, time);
                // handle executor messages to self
                let to_executors =
                    executor.to_executors_iter().collect::<Vec<_>>();
                for (shard_id, info) in to_executors {
                    assert_eq!(shard_id, DEFAULT_SHARD_ID);
                    executor.handle(info, time);
                }
                // TODO remove collect
                executor.to_clients_iter().collect::<Vec<_>>()
            })
            // handle all partial results in pending
            .filter_map(|executor_result| {
                pending.add_executor_result(executor_result)
            })
            .collect();

        // schedule new messages
        self.schedule_protocol_actions(
            process_id,
            shard_id,
            MessageRegion::Process(process_id),
            protocol_actions,
        );

        // schedule new command results
        ready.into_iter().for_each(|cmd_result| {
            self.schedule_to_client(
                MessageRegion::Process(process_id),
                cmd_result,
            )
        });
    }

    /// (maybe) Schedules a new send from some process.
    fn schedule_protocol_actions(
        &mut self,
        process_id: ProcessId,
        shard_id: ShardId,
        from_region: MessageRegion,
        protocol_actions: Vec<Action<P>>,
    ) {
        for protocol_action in protocol_actions {
            match protocol_action {
                Action::ToSend { target, msg } => {
                    // for each process in target, schedule message delivery
                    target.into_iter().for_each(|to| {
                        // if message to self, deliver immediately
                        if to == process_id {
                            self.handle_send_to_proc(
                                process_id,
                                shard_id,
                                process_id,
                                msg.clone(),
                            )
                        } else {
                            // otherwise, create action and schedule it
                            let action = ScheduleAction::SendToProc(
                                process_id,
                                shard_id,
                                to,
                                msg.clone(),
                            );
                            self.schedule_message(
                                from_region.clone(),
                                MessageRegion::Process(to),
                                action,
                            );
                        }
                    });
                }
                Action::ToForward { msg } => {
                    // deliver to-forward messages immediately
                    self.handle_send_to_proc(
                        process_id, shard_id, process_id, msg,
                    );
                }
            }
        }
    }

    /// Schedules a new command result.
    fn schedule_to_client(
        &mut self,
        from_region: MessageRegion,
        cmd_result: CommandResult,
    ) {
        // create action and schedule it
        let client_id = cmd_result.rifl().source();
        let action = ScheduleAction::SendToClient(client_id, cmd_result);
        self.schedule_message(
            from_region,
            MessageRegion::Client(client_id),
            action,
        );
    }

    /// Schedules a message.
    fn schedule_message(
        &mut self,
        from_region: MessageRegion,
        to_region: MessageRegion,
        action: ScheduleAction<P::Message, P::PeriodicEvent>,
    ) {
        // get actual regions
        let from = self.compute_region(from_region);
        let to = self.compute_region(to_region);
        // compute distance between regions
        let mut distance = self.distance(from, to);

        // check if we should reorder messages
        if self.reorder_messages {
            // if so, multiply distance by some random number between 0 and 10
            let multiplier: f64 = rand::thread_rng().gen_range(0.0..10.0);
            distance = (distance as f64 * multiplier) as u64;
        }

        // schedule action
        let distance = Duration::from_millis(distance);
        self.schedule
            .schedule(self.simulation.time(), distance, action);
    }

    /// Schedules the next periodic process event.
    fn schedule_periodic_process_event(
        &mut self,
        process_id: ProcessId,
        event: P::PeriodicEvent,
        delay: Duration,
    ) {
        // create action
        let action =
            ScheduleAction::PeriodicProcessEvent(process_id, event, delay);
        self.schedule
            .schedule(self.simulation.time(), delay, action);
    }

    /// Schedules the next periodic executed notification.
    fn schedule_periodic_executed_notification(
        &mut self,
        process_id: ProcessId,
        delay: Duration,
    ) {
        // create action
        let action =
            ScheduleAction::PeriodicExecutedNotification(process_id, delay);
        self.schedule
            .schedule(self.simulation.time(), delay, action);
    }

    /// Retrieves the region of some process/client.
    fn compute_region(&self, message_region: MessageRegion) -> &Region {
        match message_region {
            MessageRegion::Process(process_id) => self
                .process_to_region
                .get(&process_id)
                .expect("process region should be known"),
            MessageRegion::Client(client_id) => self
                .client_to_region
                .get(&client_id)
                .expect("client region should be known"),
        }
    }

    /// Computes the distance between two regions which is half the ping
    /// latency.
    fn distance(&self, from: &Region, to: &Region) -> u64 {
        let from_to = self
            .planet
            .ping_latency(from, to)
            .expect("both regions should exist on the planet");

        // compute ping time (maybe make it symmetric)
        let ping = if self.make_distances_symmetric {
            let to_from = self
                .planet
                .ping_latency(to, from)
                .expect("both regions should exist on the planet");
            (from_to + to_from) / 2
        } else {
            from_to
        };

        // distance is half the ping latency
        let ms = ping / 2;
        ms
    }

    /// Get metrics from processes and executors.
    /// TODO does this need to be mut?
    fn metrics(
        &mut self,
    ) -> HashMap<ProcessId, (ProtocolMetrics, ExecutorMetrics)> {
        self.check_processes_and_executors(|process, executor| {
            let process_metrics = process.metrics().clone();
            let executor_metrics = executor.metrics().clone();
            (process_metrics, executor_metrics)
        })
    }

    fn executors_monitors(
        &mut self,
    ) -> HashMap<ProcessId, Option<ExecutionOrderMonitor>> {
        self.check_processes_and_executors(|_process, executor| {
            executor.monitor()
        })
    }

    /// Get client's stats.
    /// TODO does this need to be mut?
    fn clients_latencies(&mut self) -> HashMap<Region, (usize, Histogram)> {
        self.check_clients(
            |client, (commands, histogram): &mut (usize, Histogram)| {
                // update issued commands with this client's issued commands
                *commands += client.issued_commands();

                // update region's histogram with this client's histogram
                for latency in client.data().latency_data() {
                    // since the simulation assumes WAN, use milliseconds for
                    // latency precision
                    let ms = latency.as_millis() as u64;
                    histogram.increment(ms);
                }
            },
        )
    }

    fn check_processes_and_executors<F, R>(
        &mut self,
        f: F,
    ) -> HashMap<ProcessId, R>
    where
        F: Fn(&P, &P::Executor) -> R,
    {
        let simulation = &mut self.simulation;

        self.process_to_region
            .keys()
            .map(|&process_id| {
                // get process and executor from simulation
                let (process, executor, _, _) =
                    simulation.get_process(process_id);

                // compute process result
                (process_id, f(&process, &executor))
            })
            .collect()
    }

    fn check_clients<F, R>(&mut self, f: F) -> HashMap<Region, R>
    where
        F: Fn(&Client, &mut R),
        R: Default,
    {
        let simulation = &mut self.simulation;
        let mut region_to_results = HashMap::new();

        for (&client_id, region) in self.client_to_region.iter() {
            // get current result for this region
            let mut result = match region_to_results.get_mut(region) {
                Some(v) => v,
                None => region_to_results.entry(region.clone()).or_default(),
            };

            // get client from simulation
            let (client, _) = simulation.get_client(client_id);

            // update region result
            f(&client, &mut result);
        }

        region_to_results
    }
}

impl<Message: Debug, PeriodicEvent: Debug> fmt::Debug
    for ScheduleAction<Message, PeriodicEvent>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScheduleAction::SubmitToProc(process_id, cmd) => {
                write!(f, "SubmitToProc({}, {:?})", process_id, cmd)
            }
            ScheduleAction::SendToProc(
                from_process_id,
                from_shard_id,
                to,
                msg,
            ) => write!(
                f,
                "SendToProc({}, {}, {}, {:?})",
                from_process_id, from_shard_id, to, msg
            ),
            ScheduleAction::SendToClient(client_id, cmd_result) => {
                write!(f, "SendToClient({}, {:?})", client_id, cmd_result)
            }
            ScheduleAction::PeriodicProcessEvent(process_id, event, delay) => {
                write!(
                    f,
                    "PeriodicProcessEvent({}, {:?}, {:?})",
                    process_id, event, delay
                )
            }
            ScheduleAction::PeriodicExecutedNotification(process_id, delay) => {
                write!(
                    f,
                    "PeriodicExecutedNotification({}, {:?})",
                    process_id, delay
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::KeyGen;
    use crate::metrics::F64;
    use crate::protocol::{Basic, ProtocolMetricsKind};

    fn run(f: usize, clients_per_process: usize) -> (Histogram, Histogram) {
        // planet
        let planet = Planet::new();

        // config
        let n = 3;
        let mut config = Config::new(n, f);

        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // clients workload
        let shard_count = 1;
        let keys_per_command = 1;
        let pool_size = 1;
        let conflict_rate = 100;
        let key_gen = KeyGen::ConflictPool {
            pool_size,
            conflict_rate,
        };
        let commands_per_client = 1000;
        let payload_size = 100;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );

        // process regions
        let process_regions = vec![
            Region::new("asia-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
        ];

        // client regions
        let client_regions =
            vec![Region::new("us-west1"), Region::new("us-west2")];

        // create runner
        let mut runner: Runner<Basic> = Runner::new(
            planet,
            config,
            workload,
            clients_per_process,
            process_regions,
            client_regions,
        );

        // run simulation until the clients end + another second second
        let (metrics, _executors_monitors, mut clients_latencies) =
            runner.run(Some(Duration::from_secs(1)));

        // check client stats
        let (us_west1_issued, us_west1) = clients_latencies
            .remove(&Region::new("us-west1"))
            .expect("there should stats from us-west1 region");
        let (us_west2_issued, us_west2) = clients_latencies
            .remove(&Region::new("us-west2"))
            .expect("there should stats from us-west2 region");

        // check the number of issued commands
        let expected = commands_per_client * clients_per_process;
        assert_eq!(us_west1_issued, expected);
        assert_eq!(us_west2_issued, expected);

        // check process stats
        metrics.values().into_iter().for_each(
            |(process_metrics, _executor_metrics)| {
                // check stability has run
                let stable_count = process_metrics
                    .get_aggregated(ProtocolMetricsKind::Stable)
                    .expect("stability should have happened");

                // check that all commands were gc-ed:
                // - since we have clients in two regions, the total number of
                //   commands is two times the expected per region
                let total_commands = (expected * 2) as u64;
                assert!(*stable_count == total_commands)
            },
        );

        // return stats for both regions
        (us_west1, us_west2)
    }

    #[test]
    fn runner_single_client_per_process() {
        // expected stats:
        // - client us-west1: since us-west1 is a process, from client's
        //   perspective it should be the latency of accessing the coordinator
        //   (0ms) plus the latency of accessing the closest fast quorum
        // - client us-west2: since us-west2 is _not_ a process, from client's
        //   perspective it should be the latency of accessing the coordinator
        //   us-west1 (12ms + 12ms) plus the latency of accessing the closest
        //   fast quorum

        // clients per process
        let clients_per_process = 1;

        // f = 0
        let f = 0;
        let (us_west1, us_west2) = run(f, clients_per_process);
        assert_eq!(us_west1.mean(), F64::new(0.0));
        assert_eq!(us_west2.mean(), F64::new(24.0));

        // f = 1
        let f = 1;
        let (us_west1, us_west2) = run(f, clients_per_process);
        assert_eq!(us_west1.mean(), F64::new(34.0));
        assert_eq!(us_west2.mean(), F64::new(58.0));

        // f = 2
        let f = 2;
        let (us_west1, us_west2) = run(f, clients_per_process);
        assert_eq!(us_west1.mean(), F64::new(118.0));
        assert_eq!(us_west2.mean(), F64::new(142.0));
    }

    #[test]
    fn runner_multiple_clients_per_process() {
        // 1 client per region
        let f = 1;
        let clients_per_process = 1;
        let (us_west1_with_one, us_west2_with_one) =
            run(f, clients_per_process);

        // 10 clients per region
        let f = 1;
        let clients_per_process = 10;
        let (us_west1_with_ten, us_west2_with_ten) =
            run(f, clients_per_process);

        // check stats are the same
        assert_eq!(us_west1_with_one.mean(), us_west1_with_ten.mean());
        assert_eq!(us_west1_with_one.cov(), us_west1_with_ten.cov());
        assert_eq!(us_west2_with_one.mean(), us_west2_with_ten.mean());
        assert_eq!(us_west2_with_one.cov(), us_west2_with_ten.cov());
    }
}
