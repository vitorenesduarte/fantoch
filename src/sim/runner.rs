use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, ProcessId};
use crate::metrics::Stats;
use crate::planet::{Planet, Region};
use crate::protocol::{Process, ToSend};
use crate::sim::{Schedule, Simulation};
use crate::time::SimTime;
use std::collections::HashMap;

enum ScheduleAction<P: Process> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, ProcessId, P::Message),
    SendToClient(ClientId, CommandResult),
}

#[derive(Clone)]
enum MessageRegion {
    Process(ProcessId),
    Client(ClientId),
}

pub struct Runner<P: Process> {
    planet: Planet,
    simulation: Simulation<P>,
    time: SimTime,
    schedule: Schedule<ScheduleAction<P>>,
    // mapping from process identifier to its region
    process_to_region: HashMap<ProcessId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
}

impl<P> Runner<P>
where
    P: Process,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
    /// - `process_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new<F>(
        planet: Planet,
        config: Config,
        create_process: F,
        workload: Workload,
        clients_per_region: usize,
        process_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self
    where
        F: Fn(ProcessId, Region, Planet, Config) -> P,
    {
        // check that we have the correct number of `process_regions`
        assert_eq!(process_regions.len(), config.n());

        // create simulation
        let mut simulation = Simulation::new();
        let mut processes = Vec::with_capacity(config.n());

        // create processes
        let to_discover: Vec<_> = process_regions
            .into_iter()
            .zip(1..=config.n())
            .map(|(region, process_id)| {
                let process_id = process_id as u64;
                // create process and save it
                let process = create_process(process_id, region.clone(), planet.clone(), config);
                processes.push(process);

                (process_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover.clone().into_iter().collect();

        // register processes
        processes.into_iter().for_each(|mut process| {
            // discover
            assert!(process.discover(to_discover.clone()));
            // and register it
            simulation.register_process(process);
        });

        // register clients and create client to region mapping
        let mut client_id = 0;
        let mut client_to_region = HashMap::new();
        for region in client_regions {
            for _ in 1..=clients_per_region {
                // create client
                client_id += 1;
                let mut client = Client::new(client_id, region.clone(), planet.clone(), workload);
                // discover
                assert!(client.discover(to_discover.clone()));
                // and register it
                simulation.register_client(client);
                client_to_region.insert(client_id, region.clone());
            }
        }

        // create runner
        Self {
            planet,
            simulation,
            time: SimTime::new(),
            schedule: Schedule::new(),
            process_to_region,
            client_to_region,
        }
    }

    /// Run the simulation.
    pub fn run(&mut self) -> HashMap<&Region, (usize, Stats<u64>)> {
        // start clients
        self.simulation
            .start_clients(&self.time)
            .into_iter()
            .for_each(|(client_id, to_send)| {
                // schedule client commands
                self.try_to_schedule(MessageRegion::Client(client_id), to_send);
            });

        // run simulation loop
        self.simulation_loop();

        // show processes stats
        self.processes_stats();

        // return clients stats
        self.clients_stats()
    }

    fn simulation_loop(&mut self) {
        // run the simulation while there are things scheduled
        while let Some(actions) = self.schedule.next_actions(&mut self.time) {
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SubmitToProc(process_id, cmd) => {
                        // submit to process and schedule output messages
                        let to_send = self.simulation.get_process(process_id).submit(cmd);
                        self.try_to_schedule(MessageRegion::Process(process_id), to_send);
                    }
                    ScheduleAction::SendToProc(from, process_id, msg) => {
                        // get process
                        let process = self.simulation.get_process(process_id);

                        // handle message and get ready commands
                        let to_send = process.handle(from, msg);
                        let ready = process.commands_ready();

                        // schedule new messages
                        self.try_to_schedule(MessageRegion::Process(process_id), to_send);

                        // schedule new command results
                        ready.into_iter().for_each(|cmd_result| {
                            // create action and schedule it
                            let client_id = cmd_result.rifl().source();
                            let action = ScheduleAction::SendToClient(client_id, cmd_result);
                            self.schedule_it(
                                MessageRegion::Process(process_id),
                                MessageRegion::Client(client_id),
                                action,
                            );
                        });
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // route to client and schedule output command
                        let to_send = self
                            .simulation
                            .forward_to_client(client_id, cmd_result, &self.time);
                        self.try_to_schedule(MessageRegion::Client(client_id), to_send);
                    }
                }
            })
        }
    }

    /// Try to schedule a `ToSend`.
    fn try_to_schedule(&mut self, from_region: MessageRegion, to_send: ToSend<P::Message>) {
        match to_send {
            ToSend::ToCoordinator(process_id, cmd) => {
                // create action and schedule it
                let action = ScheduleAction::SubmitToProc(process_id, cmd);
                self.schedule_it(from_region, MessageRegion::Process(process_id), action);
            }
            ToSend::ToProcesses(from, target, msg) => {
                // for each process in target, schedule message delivery
                target.into_iter().for_each(|to| {
                    // otherwise, create action and schedule it
                    let action = ScheduleAction::SendToProc(from, to, msg.clone());
                    self.schedule_it(from_region.clone(), MessageRegion::Process(to), action);
                });
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
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
        self.schedule.schedule(&self.time, distance, action);
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

    /// Computes the distance between two regions which is half the ping latency.
    fn distance(&self, from: &Region, to: &Region) -> u64 {
        let ping_latency = self
            .planet
            .ping_latency(from, to)
            .expect("both regions should exist on the planet");
        // distance is half the ping latency
        ping_latency / 2
    }

    /// Get client's stats.
    /// TODO does this need to be mut?
    fn clients_stats(&mut self) -> HashMap<&Region, (usize, Stats<u64>)> {
        let simulation = &mut self.simulation;
        let mut region_to_latencies = HashMap::new();

        for (&client_id, region) in self.client_to_region.iter() {
            // get client from simulation
            let client = simulation.get_client(client_id);
            // get client's issued commands and latencies
            let issued_commands = client.issued_commands();
            let latencies = client.latencies();

            // get current metrics from this region
            let (total_issued_commands, current_latencies) =
                region_to_latencies.entry(region).or_insert((0, Vec::new()));
            // update metrics
            *total_issued_commands += issued_commands;
            current_latencies.extend(latencies);
        }

        // compute stats for each region
        region_to_latencies
            .into_iter()
            .map(|(region, (total_issued_commands, latencies))| {
                let stats = Stats::from(latencies);
                (region, (total_issued_commands, stats))
            })
            .collect()
    }

    /// Show processes' stats.
    /// TODO does this need to be mut?
    fn processes_stats(&mut self) {
        let simulation = &mut self.simulation;
        self.process_to_region.keys().for_each(|process_id| {
            // get process from simulation
            let process = simulation.get_process(*process_id);
            println!("process {:?} stats:", process_id);
            process.show_stats();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ProcessId, Rifl};
    use crate::metrics::F64;
    use crate::protocol::BaseProcess;
    use std::mem;

    #[derive(Clone)]
    enum Message {
        Ping(Rifl),
        Pong(Rifl),
    }

    // Protocol that pings the closest f + 1 processes and returns reply to client.
    struct PingPong {
        bp: BaseProcess,
        acks: HashMap<Rifl, usize>,
        commands_ready: Vec<CommandResult>,
    }

    impl Process for PingPong {
        type Message = Message;

        fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self {
            // quorum size is f + 1
            let q = config.f() + 1;
            let bp = BaseProcess::new(process_id, region, planet, config, q);

            // create `PingPong`
            Self {
                bp,
                acks: HashMap::new(),
                commands_ready: Vec::new(),
            }
        }

        fn id(&self) -> ProcessId {
            self.bp.process_id
        }

        fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool {
            self.bp.discover(processes)
        }

        fn submit(&mut self, cmd: Command) -> ToSend<Self::Message> {
            // get rifl
            let rifl = cmd.rifl();
            // create entry in acks
            self.acks.insert(rifl, 0);

            // create message
            let msg = Message::Ping(rifl);
            // clone the fast quorum
            let fast_quorum = self
                .bp
                .fast_quorum
                .clone()
                .expect("should have a valid fast quorum upon submit");

            // send message to fast quorum
            ToSend::ToProcesses(self.id(), fast_quorum, msg)
        }

        fn handle(&mut self, from: ProcessId, msg: Self::Message) -> ToSend<Self::Message> {
            match msg {
                Message::Ping(rifl) => {
                    // create msg
                    let msg = Message::Pong(rifl);
                    // reply back
                    ToSend::ToProcesses(self.id(), vec![from], msg)
                }
                Message::Pong(rifl) => {
                    // increase ack count
                    let ack_count = self
                        .acks
                        .get_mut(&rifl)
                        .expect("there should be an ack count for this rifl");
                    *ack_count += 1;

                    // notify client if enough acks
                    if *ack_count == self.bp.q {
                        // remove from acks
                        self.acks.remove(&rifl);
                        // create fake command result
                        let cmd_result = CommandResult::new(rifl, 0);
                        // save new ready command
                        self.commands_ready.push(cmd_result);
                    }

                    // nothing to send
                    ToSend::Nothing
                }
            }
        }

        /// Returns new commands results to be sent to clients.
        fn commands_ready(&mut self) -> Vec<CommandResult> {
            let mut ready = Vec::new();
            mem::swap(&mut ready, &mut self.commands_ready);
            ready
        }
    }

    fn run(f: usize, clients_per_region: usize) -> (Stats<u64>, Stats<u64>) {
        // planet
        let planet = Planet::new("latency/");

        // config
        let n = 3;
        let config = Config::new(n, f);

        // function that creates ping pong processes
        let create_process =
            |process_id, region, planet, config| PingPong::new(process_id, region, planet, config);

        // clients workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // process regions
        let process_regions = vec![
            Region::new("asia-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
        ];

        // client regions
        let client_regions = vec![Region::new("us-west1"), Region::new("us-west2")];

        // create runner
        let mut runner = Runner::new(
            planet,
            config,
            create_process,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );

        // run simulation and return stats
        runner.run();
        let mut stats = runner.clients_stats();
        let (us_west1_issued, us_west1) = stats
            .remove(&Region::new("us-west1"))
            .expect("there should stats from us-west1 region");
        let (us_west2_issued, us_west2) = stats
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
        // - client us-west1: since us-west1 is a process, from client's perspective it should be
        //   the latency of accessing the coordinator (0ms) plus the latency of accessing the
        //   closest fast quorum
        // - client us-west2: since us-west2 is _not_ a process, from client's perspective it should
        //   be the latency of accessing the coordinator us-west1 (12ms + 12ms) plus the latency of
        //   accessing the closest fast quorum

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
