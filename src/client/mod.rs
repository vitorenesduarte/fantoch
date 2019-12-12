// This module contains the definition of `Workload`.
pub mod workload;

// This module contains the defintion of `Pending`
pub mod pending;

// Re-exports.
pub use pending::Pending;
pub use workload::Workload;

use crate::command::{Command, CommandResult};
use crate::id::ProcId;
use crate::id::{ClientId, RiflGen};
use crate::planet::{Planet, Region};
use crate::stats::Stats;
use crate::time::SysTime;
use crate::util;

pub struct Client {
    /// id of this client
    client_id: ClientId,
    /// region where this client is
    region: Region,
    planet: Planet,
    /// id of the process this client is connected to
    proc_id: Option<ProcId>,
    /// rifl id generator
    rifl_gen: RiflGen,
    /// workload configuration
    workload: Workload,
    /// map from pending command RIFL to its start time
    pending: Pending,
    /// list of all command latencies
    latencies: Vec<u64>,
}

impl Client {
    /// Creates a new client.
    pub fn new(client_id: ClientId, region: Region, planet: Planet, workload: Workload) -> Self {
        // create client
        Self {
            client_id,
            region,
            planet,
            proc_id: None,
            rifl_gen: RiflGen::new(client_id),
            workload,
            pending: Pending::new(),
            latencies: Vec::with_capacity(workload.total_commands()),
        }
    }

    /// Returns the client identifier.
    pub fn id(&self) -> ClientId {
        self.client_id
    }

    /// Generate client's first command.
    pub fn discover(&mut self, mut procs: Vec<(ProcId, Region)>) -> bool {
        // sort `procs` by distance from `self.region`
        util::sort_procs_by_distance(&self.region, &self.planet, &mut procs);

        // set the closest process
        self.proc_id = procs.into_iter().map(|(proc_id, _)| proc_id).next();

        // check if we have a closest process
        self.proc_id.is_some()
    }

    /// Start client's workload.
    pub fn start(&mut self, time: &dyn SysTime) -> (ProcId, Command) {
        self.next_cmd(time)
            .expect("client should able to generate an operation when it is first started")
    }

    /// Handle executed command and its overall latency.
    pub fn handle(
        &mut self,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> Option<(ProcId, Command)> {
        // end command in pending and save command latency
        let latency = self.pending.end(cmd_result.rifl(), time);
        self.latencies.push(latency);

        // generate command
        self.next_cmd(time)
    }

    /// Computes `Stats` from latencies registered until now.
    pub fn stats(&self) -> Stats {
        Stats::from(&self.latencies)
    }

    fn next_cmd(&mut self, time: &dyn SysTime) -> Option<(ProcId, Command)> {
        self.proc_id.and_then(|proc_id| {
            // generate next command in the workload if some proc_id
            self.workload.next_cmd(&mut self.rifl_gen).map(|cmd| {
                // if a new command was generated, start it in pending
                self.pending.start(cmd.rifl(), time);
                (proc_id, cmd)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::F64;
    use crate::time::SimTime;

    // Generates some client.
    fn gen_client(total_commands: usize, region: Region) -> Client {
        // workload
        let conflict_rate = 100;
        let workload = Workload::new(conflict_rate, total_commands);

        // client
        let id = 1;
        let planet = Planet::new("latency/");
        Client::new(id, region, planet, workload)
    }

    #[test]
    fn discover() {
        // procs
        let procs = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("australia-southeast1")),
            (2, Region::new("europe-west1")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 0;
        let mut client = gen_client(total_commands, region);

        // check discover with empty vec
        assert!(!client.discover(vec![]));
        assert_eq!(client.proc_id, None);

        // check discover with procs
        assert!(client.discover(procs));
        assert_eq!(client.proc_id, Some(2));
    }

    #[test]
    #[should_panic]
    fn start_before_discover() {
        // client
        let region = Region::new("europe-west2");
        let total_commands = 10;
        let mut client = gen_client(total_commands, region);

        // create system time
        let time = SimTime::new();

        // should panic!
        client.start(&time);
    }

    #[test]
    fn client_flow() {
        // procs
        let procs = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("australia-southeast1")),
            (2, Region::new("europe-west1")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 2;
        let mut client = gen_client(total_commands, region);

        // discover
        client.discover(procs);

        // create system time
        let mut time = SimTime::new();

        // creates a fake command result from a command
        let fake_result = |cmd: Command| CommandResult::new(cmd.rifl(), 0);

        // start client at time 0
        let (proc_id, cmd) = client.start(&time);
        // proc_id should be 2
        assert_eq!(proc_id, 2);

        // handle result at time 10
        time.tick(10);
        let next = client.handle(fake_result(cmd), &time);

        // check there's next command
        assert!(next.is_some());
        let (proc_id, cmd) = next.unwrap();
        // proc_id should be 2
        assert_eq!(proc_id, 2);

        // handle result at time 15
        time.tick(5);
        let next = client.handle(fake_result(cmd), &time);

        // check there's no next command
        assert!(next.is_none());

        // check latencies
        assert_eq!(client.latencies, vec![10, 5]);

        // check stats
        let stats = client.stats();
        assert_eq!(stats.mean(), F64::new(7.5));
    }
}
