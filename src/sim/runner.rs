use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{ClientId, ProcessId};
use crate::metrics::Histogram;
use crate::planet::{Planet, Region};
use crate::protocol::{Protocol, ToSend};
use crate::sim::{Schedule, Simulation};
use crate::time::SimTime;
use std::collections::HashMap;

enum ScheduleAction<P: Protocol> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, ProcessId, P::Message),
    SendToClient(ClientId, CommandResult),
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
}

impl<P> Runner<P>
where
    P: Protocol,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
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

        // create processes
        let to_discover: Vec<_> = process_regions
            .into_iter()
            .zip(1..=config.n())
            .map(|(region, process_id)| {
                let process_id = process_id as u64;
                // create process and save it
                let process = P::new(process_id, config);
                processes.push((region.clone(), process));

                (process_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover.clone().into_iter().collect();

        // register processes
        processes.into_iter().for_each(|(region, mut process)| {
            // discover
            assert!(process.discover(&region, &planet, to_discover.clone()));

            // create executor for this process
            let executor = <P::Executor as Executor>::new(&config);

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
                assert!(client.discover(&region, &planet, to_discover.clone()));
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
    pub fn run(&mut self) -> HashMap<Region, (usize, Histogram)> {
        // start clients
        self.simulation
            .start_clients(&self.time)
            .into_iter()
            .for_each(|(client_id, submit)| {
                // schedule client commands
                self.schedule_submit(MessageRegion::Client(client_id), submit)
            });

        // run simulation loop
        self.simulation_loop();

        // show processes metrics
        self.processes_metrics();

        // return clients latencies
        self.clients_latencies()
    }

    fn simulation_loop(&mut self) {
        // run the simulation while there are things scheduled
        while let Some(actions) = self.schedule.next_actions(&mut self.time) {
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SubmitToProc(process_id, cmd) => {
                        // get process and executor
                        let (process, executor) = self.simulation.get_process(process_id);

                        // register command in the executor
                        executor.register(&cmd);

                        // submit to process and schedule output messages
                        let to_send = process.submit(cmd);
                        self.schedule_send(MessageRegion::Process(process_id), Some(to_send));
                    }
                    ScheduleAction::SendToProc(from, process_id, msg) => {
                        // get process and executor
                        let (process, executor) = self.simulation.get_process(process_id);

                        // handle message and get ready commands
                        let to_send = process.handle(from, msg);

                        // handle new execution info in the executor
                        let to_executor = process.to_executor();
                        let ready = executor.handle(to_executor);

                        // schedule new messages
                        self.schedule_send(MessageRegion::Process(process_id), to_send);

                        // schedule new command results
                        ready.into_iter().for_each(|cmd_result| {
                            self.schedule_to_client(MessageRegion::Process(process_id), cmd_result)
                        });
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // route to client and schedule new submit
                        let submit = self
                            .simulation
                            .get_client(client_id)
                            .handle(cmd_result, &self.time);
                        self.schedule_submit(MessageRegion::Client(client_id), submit);
                    }
                }
            })
        }
    }

    // (maybe) Schedules a new submit from a client.
    fn schedule_submit(
        &mut self,
        from_region: MessageRegion,
        submit: Option<(ProcessId, Command)>,
    ) {
        if let Some((process_id, cmd)) = submit {
            // create action and schedule it
            let action = ScheduleAction::SubmitToProc(process_id, cmd);
            self.schedule_it(from_region, MessageRegion::Process(process_id), action);
        }
    }

    /// (maybe) Schedules a new send from some process.
    fn schedule_send(&mut self, from_region: MessageRegion, to_send: Option<ToSend<P::Message>>) {
        if let Some(ToSend { from, target, msg }) = to_send {
            // for each process in target, schedule message delivery
            target.into_iter().for_each(|to| {
                // otherwise, create action and schedule it
                let action = ScheduleAction::SendToProc(from, to, msg.clone());
                self.schedule_it(from_region.clone(), MessageRegion::Process(to), action);
            });
        }
    }

    /// Schedules a new command result.
    fn schedule_to_client(&mut self, from_region: MessageRegion, cmd_result: CommandResult) {
        // create action and schedule it
        let client_id = cmd_result.rifl().source();
        let action = ScheduleAction::SendToClient(client_id, cmd_result);
        self.schedule_it(from_region, MessageRegion::Client(client_id), action);
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

    /// Computes the distance between two regions which is half the ping latency.
    fn distance(&self, from: &Region, to: &Region) -> u64 {
        let ping_latency = self
            .planet
            .ping_latency(from, to)
            .expect("both regions should exist on the planet");
        // distance is half the ping latency
        ping_latency / 2
    }

    /// Show processes' stats.
    /// TODO does this need to be mut?
    fn processes_metrics(&mut self) {
        let simulation = &mut self.simulation;
        self.process_to_region.keys().for_each(|process_id| {
            // get process from simulation
            let (process, executor) = simulation.get_process(*process_id);
            println!("process {:?} stats:", process_id);
            process.show_metrics();
            executor.show_metrics();
        });
    }

    /// Get client's stats.
    /// TODO does this need to be mut?
    fn clients_latencies(&mut self) -> HashMap<Region, (usize, Histogram)> {
        let simulation = &mut self.simulation;
        let mut region_to_latencies = HashMap::new();

        for (&client_id, region) in self.client_to_region.iter() {
            // get current metrics from this region
            let (total_issued_commands, histogram) = match region_to_latencies.get_mut(region) {
                Some(v) => v,
                None => region_to_latencies
                    .entry(region.clone())
                    .or_insert((0, Histogram::new())),
            };

            // get client from simulation
            let client = simulation.get_client(client_id);

            // update issued comamnds with this client's issued commands
            *total_issued_commands += client.issued_commands();

            // update region's histogram with this client's histogram
            histogram.merge(client.latency_histogram());
        }

        region_to_latencies
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::id::{ProcessId, Rifl};
    use crate::metrics::F64;
    use crate::protocol::BaseProcess;
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;
    use std::mem;

    type ExecutionInfo = <PingPongExecutor as Executor>::ExecutionInfo;

    struct PingPongExecutor {
        pending: HashSet<Rifl>,
    }

    impl PingPongExecutor {
        fn new() -> Self {
            PingPongExecutor {
                pending: HashSet::new(),
            }
        }
    }

    impl Executor for PingPongExecutor {
        type ExecutionInfo = CommandResult;

        fn new(_config: &Config) -> Self {
            // ignore config
            PingPongExecutor::new()
        }

        fn register(&mut self, cmd: &Command) {
            // add to pending and make sure it was not there
            assert!(self.pending.insert(cmd.rifl()));
        }

        fn handle(&mut self, infos: Vec<CommandResult>) -> Vec<CommandResult> {
            infos
                .into_iter()
                .filter(|info| {
                    // only return those results that belong to registered commands
                    self.pending.remove(&info.rifl())
                })
                .collect()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum Message {
        Ping(Rifl),
        Pong(Rifl),
    }

    // Protocol that pings the closest f + 1 processes and returns reply to client.
    struct PingPong {
        bp: BaseProcess,
        acks: HashMap<Rifl, usize>,
        to_executor: Vec<ExecutionInfo>,
    }

    impl Protocol for PingPong {
        type Message = Message;
        type Executor = PingPongExecutor;

        fn new(process_id: ProcessId, config: Config) -> Self {
            // fast quorum size is f + 1
            let fast_quorum_size = config.f() + 1;
            // write quorum size can be 0 since it won't be used
            let write_quorum_size = 0;
            let bp = BaseProcess::new(process_id, config, fast_quorum_size, write_quorum_size);

            // create `PingPong`
            Self {
                bp,
                acks: HashMap::new(),
                to_executor: Vec::new(),
            }
        }

        fn id(&self) -> ProcessId {
            self.bp.process_id
        }

        fn discover(
            &mut self,
            region: &Region,
            planet: &Planet,
            processes: Vec<(ProcessId, Region)>,
        ) -> bool {
            self.bp.discover(region, planet, processes)
        }

        fn submit(&mut self, cmd: Command) -> ToSend<Self::Message> {
            // get rifl
            let rifl = cmd.rifl();
            // create entry in acks
            self.acks.insert(rifl, 0);

            // create message
            let msg = Message::Ping(rifl);
            // get the fast quorum
            let fast_quorum = self.bp.fast_quorum();

            // send message to fast quorum
            ToSend {
                from: self.id(),
                target: fast_quorum,
                msg,
            }
        }

        fn handle(&mut self, from: ProcessId, msg: Self::Message) -> Option<ToSend<Self::Message>> {
            match msg {
                Message::Ping(rifl) => {
                    // create msg
                    let msg = Message::Pong(rifl);
                    // reply back
                    Some(ToSend {
                        from: self.id(),
                        target: vec![from],
                        msg,
                    })
                }
                Message::Pong(rifl) => {
                    // increase ack count
                    let ack_count = self
                        .acks
                        .get_mut(&rifl)
                        .expect("there should be an ack count for this rifl");
                    *ack_count += 1;

                    // notify client if enough acks
                    if *ack_count == self.bp.fast_quorum().len() {
                        // remove from acks
                        self.acks.remove(&rifl);
                        // create fake command result
                        let ready = CommandResult::new(rifl, 0);
                        self.to_executor.push(ready);
                    }
                    // nothing to send
                    None
                }
            }
        }

        /// Returns new commands results to be sent to clients.
        fn to_executor(&mut self) -> Vec<ExecutionInfo> {
            let mut to_executor = Vec::new();
            mem::swap(&mut to_executor, &mut self.to_executor);
            to_executor
        }
    }

    fn run(f: usize, clients_per_region: usize) -> (Histogram, Histogram) {
        // planet
        let planet = Planet::new("latency/");

        // config
        let n = 3;
        let config = Config::new(n, f);

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
        let mut runner: Runner<PingPong> = Runner::new(
            planet,
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );

        // run simulation and return stats
        runner.run();
        let mut latencies = runner.clients_latencies();
        let (us_west1_issued, us_west1) = latencies
            .remove(&Region::new("us-west1"))
            .expect("there should stats from us-west1 region");
        let (us_west2_issued, us_west2) = latencies
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
