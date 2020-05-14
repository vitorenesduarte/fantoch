use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::metrics::Histogram;
use crate::planet::{Planet, Region};
use crate::protocol::{Protocol, ProtocolMetrics, ToSend};
use crate::sim::{Schedule, Simulation};
use crate::time::SimTime;
use crate::util;
use std::collections::HashMap;

enum ScheduleAction<P: Protocol> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, ProcessId, P::Message),
    SendToClient(ClientId, CommandResult),
    PeriodicEvent(ProcessId, P::PeriodicEvent, u128),
}

#[derive(Clone)]
enum MessageRegion {
    Process(ProcessId),
    Client(ClientId),
}

pub struct Runner<P: Protocol> {
    planet: Planet,
    simulation: Simulation<P>,
    time: SimTime,
    schedule: Schedule<ScheduleAction<P>>,
    // mapping from process identifier to its region
    process_to_region: HashMap<ProcessId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
    // total number of clients
    client_count: usize,
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
        clients_per_region: usize,
        process_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self {
        // check that we have the correct number of `process_regions`
        assert_eq!(process_regions.len(), config.n());

        // create simulation
        let mut simulation = Simulation::new();
        let mut processes = Vec::with_capacity(config.n());
        let mut periodic_actions = Vec::new();

        // create processes
        let to_discover: Vec<_> = process_regions
            .into_iter()
            .zip(1..=config.n())
            .map(|(region, process_id)| {
                let process_id = process_id as u64;
                // create process and save it
                let (process, process_events) = P::new(process_id, config);
                processes.push((region.clone(), process));

                // save periodic actions
                // - map the delay from milliseconds from microseconds
                periodic_actions.extend(process_events.into_iter().map(
                    |(event, delay_milli)| {
                        let delay_micro = (delay_milli * 1000) as u128;
                        (process_id, event, delay_micro)
                    },
                ));

                (process_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover.clone().into_iter().collect();

        // register processes
        processes.into_iter().for_each(|(region, mut process)| {
            // discover
            let sorted = util::sort_processes_by_distance(
                &region,
                &planet,
                to_discover.clone(),
            );
            assert!(process.discover(sorted));

            // create executor for this process
            let executor = <P::Executor as Executor>::new(config);

            // and register both
            simulation.register_process(process, executor);
        });

        // register clients and create client to region mapping
        let mut client_id = 0;
        let mut client_to_region = HashMap::new();
        for region in client_regions {
            for _ in 1..=clients_per_region {
                // create client
                client_id += 1;
                let mut client = Client::new(client_id, workload);
                // discover
                let sorted = util::sort_processes_by_distance(
                    &region,
                    &planet,
                    to_discover.clone(),
                );
                assert!(client.discover(sorted));
                // and register it
                simulation.register_client(client);
                client_to_region.insert(client_id, region.clone());
            }
        }

        // create runner
        let mut runner = Self {
            planet,
            simulation,
            time: SimTime::new(),
            schedule: Schedule::new(),
            process_to_region,
            client_to_region,
            // since we start ids in 1, the last id is the same as the numbe of
            // clients
            client_count: client_id as usize,
        };

        // schedule periodic actions
        for (process_id, event, delay) in periodic_actions {
            runner.schedule_event(process_id, event, delay);
        }

        runner
    }

    /// Run the simulation.
    pub fn run(
        &mut self,
    ) -> (
        HashMap<ProcessId, ProtocolMetrics>,
        HashMap<Region, (usize, Histogram)>,
    ) {
        // start clients
        self.simulation
            .start_clients(&self.time)
            .into_iter()
            .for_each(|(client_id, process_id, cmd)| {
                // schedule client commands
                self.schedule_submit(
                    MessageRegion::Client(client_id),
                    process_id,
                    cmd,
                )
            });

        // run simulation loop
        self.simulation_loop();

        // return processes metrics and client latencies
        (self.processes_metrics(), self.clients_latencies())
    }

    fn simulation_loop(&mut self) {
        let mut clients_done = 0;
        // run the simulation while clients don't finish
        // run the simulation while there are things scheduled
        while clients_done != self.client_count {
            let actions = self.schedule.next_actions(&mut self.time).expect("there should be more actions since clients have not finished yet");
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SubmitToProc(process_id, cmd) => {
                        // get process and executor
                        let (process, executor) =
                            self.simulation.get_process(process_id);

                        // register command in the executor
                        executor.wait_for(&cmd);

                        // submit to process and schedule output messages
                        let to_send = process.submit(None, cmd);
                        self.schedule_send(
                            MessageRegion::Process(process_id),
                            Some(to_send),
                        );
                    }
                    ScheduleAction::SendToProc(from, process_id, msg) => {
                        // get process and executor
                        let (process, executor) =
                            self.simulation.get_process(process_id);

                        // handle message and get ready commands
                        let to_send = process.handle(from, msg);

                        // handle new execution info in the executor
                        let to_executor = process.to_executor();
                        let ready: Vec<_> = to_executor
                            .into_iter()
                            .flat_map(|info| executor.handle(info))
                            .map(|result| result.unwrap_ready())
                            .collect();

                        // schedule new messages
                        self.schedule_send(
                            MessageRegion::Process(process_id),
                            to_send,
                        );

                        // schedule new command results
                        ready.into_iter().for_each(|cmd_result| {
                            self.schedule_to_client(
                                MessageRegion::Process(process_id),
                                cmd_result,
                            )
                        });
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // handle new command result in client
                        let submit = self
                            .simulation
                            .forward_to_client(cmd_result, &self.time);
                        if let Some((process_id, cmd)) = submit {
                            self.schedule_submit(
                                MessageRegion::Client(client_id),
                                process_id,
                                cmd,
                            );
                        } else {
                            clients_done += 1;
                        }
                    }
                    ScheduleAction::PeriodicEvent(process_id, event, delay) => {
                        // get process
                        let (process, _) =
                            self.simulation.get_process(process_id);

                        // handle event
                        for to_send in process.handle_event(event.clone()) {
                            self.schedule_send(
                                MessageRegion::Process(process_id),
                                Some(to_send.clone()),
                            );
                        }

                        // schedule the next periodic event
                        self.schedule_event(process_id, event, delay);
                    }
                }
            })
        }
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
        self.schedule_it(
            from_region,
            MessageRegion::Process(process_id),
            action,
        );
    }

    /// (maybe) Schedules a new send from some process.
    fn schedule_send(
        &mut self,
        from_region: MessageRegion,
        to_send: Option<ToSend<P::Message>>,
    ) {
        if let Some(ToSend { from, target, msg }) = to_send {
            // for each process in target, schedule message delivery
            target.into_iter().for_each(|to| {
                // otherwise, create action and schedule it
                let action = ScheduleAction::SendToProc(from, to, msg.clone());
                self.schedule_it(
                    from_region.clone(),
                    MessageRegion::Process(to),
                    action,
                );
            });
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
        self.schedule_it(from_region, MessageRegion::Client(client_id), action);
    }

    /// Schedules the next periodic event.
    fn schedule_event(
        &mut self,
        process_id: ProcessId,
        event: P::PeriodicEvent,
        delay: u128,
    ) {
        // create action
        let action = ScheduleAction::PeriodicEvent(process_id, event, delay);
        self.schedule.schedule(&self.time, delay, action);
    }

    fn schedule_it(
        &mut self,
        from_region: MessageRegion,
        to_region: MessageRegion,
        action: ScheduleAction<P>,
    ) {
        // get actual regions
        let from = self.compute_region(from_region);
        let to = self.compute_region(to_region);
        // compute distance between regions
        let distance = self.distance(from, to);
        // schedule action
        self.schedule.schedule(&self.time, distance as u128, action);
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
        let ping_latency = self
            .planet
            .ping_latency(from, to)
            .expect("both regions should exist on the planet");
        // distance is half the ping latency
        ping_latency / 2
    }

    /// Get processes' metrics.
    /// TODO does this need to be mut?
    fn processes_metrics(&mut self) -> HashMap<ProcessId, ProtocolMetrics> {
        self.check_processes(|process| process.metrics().clone())
    }

    /// Get client's stats.
    /// TODO does this need to be mut?
    fn clients_latencies(&mut self) -> HashMap<Region, (usize, Histogram)> {
        self.check_clients(
            |client, (commands, histogram): &mut (usize, Histogram)| {
                // update issued commands with this client's issued commands
                *commands += client.issued_commands();

                // update region's histogram with this client's histogram
                histogram.merge(client.latency_histogram());
            },
        )
    }

    fn check_processes<F, R>(&mut self, f: F) -> HashMap<ProcessId, R>
    where
        F: Fn(&P) -> R,
    {
        let simulation = &mut self.simulation;

        self.process_to_region
            .keys()
            .map(|&process_id| {
                // get process from simulation
                let (process, _executor) = simulation.get_process(process_id);

                // compute process result
                (process_id, f(&process))
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
            let client = simulation.get_client(client_id);

            // update region result
            f(&client, &mut result);
        }

        region_to_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::F64;
    use crate::protocol::Basic;

    fn run(f: usize, clients_per_region: usize) -> (Histogram, Histogram) {
        // planet
        let planet = Planet::new();

        // config
        let n = 3;
        let config = Config::new(n, f);

        // clients workload
        let conflict_rate = 100;
        let total_commands = 10;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

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
            clients_per_region,
            process_regions,
            client_regions,
        );

        // run simulation
        let (processes_metrics, mut clients_latencies) = runner.run();

        let (us_west1_issued, us_west1) = clients_latencies
            .remove(&Region::new("us-west1"))
            .expect("there should stats from us-west1 region");
        let (us_west2_issued, us_west2) = clients_latencies
            .remove(&Region::new("us-west2"))
            .expect("there should stats from us-west2 region");

        // check the number of issued commands
        let expected = total_commands * clients_per_region;
        assert_eq!(us_west1_issued, expected);
        assert_eq!(us_west2_issued, expected);

        // return stats for both regions
        (us_west1, us_west2)
    }

    #[test]
    fn runner_single_client_per_region() {
        // expected stats:
        // - client us-west1: since us-west1 is a process, from client's
        //   perspective it should be the latency of accessing the coordinator
        //   (0ms) plus the latency of accessing the closest fast quorum
        // - client us-west2: since us-west2 is _not_ a process, from client's
        //   perspective it should be the latency of accessing the coordinator
        //   us-west1 (12ms + 12ms) plus the latency of accessing the closest
        //   fast quorum

        // clients per region
        let clients_per_region = 1;

        // f = 0
        let f = 0;
        let (us_west1, us_west2) = run(f, clients_per_region);
        assert_eq!(us_west1.mean(), F64::new(0.0));
        assert_eq!(us_west2.mean(), F64::new(24.0));

        // f = 1
        let f = 1;
        let (us_west1, us_west2) = run(f, clients_per_region);
        assert_eq!(us_west1.mean(), F64::new(34.0));
        assert_eq!(us_west2.mean(), F64::new(58.0));

        // f = 2
        let f = 2;
        let (us_west1, us_west2) = run(f, clients_per_region);
        assert_eq!(us_west1.mean(), F64::new(118.0));
        assert_eq!(us_west2.mean(), F64::new(142.0));
    }

    #[test]
    fn runner_multiple_clients_per_region() {
        // 1 client per region
        let f = 1;
        let clients_per_region = 1;
        let (us_west1_with_one, us_west2_with_one) = run(f, clients_per_region);

        // 10 clients per region
        let f = 1;
        let clients_per_region = 10;
        let (us_west1_with_ten, us_west2_with_ten) = run(f, clients_per_region);

        // check stats are the same
        assert_eq!(us_west1_with_one.mean(), us_west1_with_ten.mean());
        assert_eq!(us_west1_with_one.cov(), us_west1_with_ten.cov());
        assert_eq!(us_west2_with_one.mean(), us_west2_with_ten.mean());
        assert_eq!(us_west2_with_one.cov(), us_west2_with_ten.cov());
    }
}
