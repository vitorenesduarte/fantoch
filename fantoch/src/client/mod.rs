// This module contains the definition of `Workload`
pub mod workload;

// This module contains the definition of `KeyGenerator` and
// `KeyGeneratorState`.
pub mod key_gen;

// This module contains the definition of `ShardGen`.
pub mod shard_gen;

// This module contains the definition of `Pending`
pub mod pending;

// This module contains the definition of `ClientData`
pub mod data;

// Re-exports.
pub use data::ClientData;
pub use key_gen::KeyGen;
pub use pending::Pending;
pub use shard_gen::ShardGen;
pub use workload::Workload;

use crate::command::{Command, CommandResult};
use crate::id::{ClientId, ProcessId, RiflGen, ShardId};
use crate::log;
use crate::time::SysTime;
use crate::{HashMap, HashSet};
use key_gen::KeyGenState;

pub struct Client {
    /// id of this client
    client_id: ClientId,
    /// mapping from shard id to the process id of that shard this client is
    /// connected to
    connected: HashMap<ShardId, ProcessId>,
    /// rifl id generator
    rifl_gen: RiflGen,
    /// workload configuration
    workload: Workload,
    /// state needed by key generator
    key_gen_state: KeyGenState,
    /// map from pending command RIFL to its start time
    pending: Pending,
    /// mapping from
    data: ClientData,
}

impl Client {
    /// Creates a new client.
    pub fn new(client_id: ClientId, workload: Workload) -> Self {
        // create client
        Self {
            client_id,
            connected: HashMap::new(),
            rifl_gen: RiflGen::new(client_id),
            workload,
            key_gen_state: workload.key_gen().initial_state(client_id),
            pending: Pending::new(),
            data: ClientData::new(),
        }
    }

    /// Returns the client identifier.
    pub fn id(&self) -> ClientId {
        self.client_id
    }

    /// "Connect" to the closest process on each shard.
    pub fn discover(&mut self, processes: Vec<(ProcessId, ShardId)>) {
        self.connected = HashMap::new();
        for (process_id, shard_id) in processes {
            // only insert the first entry for each shard id (which will be the
            // closest)
            if !self.connected.contains_key(&shard_id) {
                self.connected.insert(shard_id, process_id);
            }
        }
    }

    /// Retrieves the closest process on this shard.
    pub fn shard_process(&self, shard_id: &ShardId) -> ProcessId {
        *self
            .connected
            .get(shard_id)
            .expect("client should be connected to all shards")
    }

    /// Generates the next command in this client's workload.
    pub fn next_cmd(
        &mut self,
        time: &dyn SysTime,
    ) -> Option<(ShardId, Command)> {
        // generate next command in the workload if some process_id
        self.workload
            .next_cmd(&mut self.rifl_gen, &mut self.key_gen_state)
            .map(|(target_shard, cmd)| {
                // if a new command was generated, start it in pending
                self.pending.start(cmd.rifl(), time);
                (target_shard, cmd)
            })
    }

    /// Handle executed command and return a boolean indicating whether we have
    /// generated all commands and receive all the corresponding command
    /// results.
    pub fn handle(
        &mut self,
        cmd_results: Vec<CommandResult>,
        time: &dyn SysTime,
    ) -> bool {
        // make sure that results belong to the same rifl
        let mut rifls = HashSet::with_capacity(1);
        for cmd_result in cmd_results {
            rifls.insert(cmd_result.rifl());
        }
        assert_eq!(rifls.len(), 1);
        let rifl = rifls.into_iter().next().unwrap();

        // end command in pending and save command latency
        let (latency, end_time) = self.pending.end(rifl, time);
        log!(
            "rifl {:?} ended after {} micros at {}",
            rifl,
            latency.as_micros(),
            end_time
        );
        self.data.record(latency, end_time);

        // we're done once:
        // - the workload is finished and
        // - pending is empty
        self.workload.finished() && self.pending.is_empty()
    }

    pub fn data(&self) -> &ClientData {
        &self.data
    }

    /// Returns the number of commands already issued.
    pub fn issued_commands(&self) -> usize {
        self.workload.issued_commands()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planet::{Planet, Region};
    use crate::time::SimTime;
    use crate::util;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;
    use std::time::Duration;

    // Generates some client.
    fn gen_client(total_commands: usize) -> Client {
        // workload
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shards: 1 };
        let keys_per_shard = 1;
        let conflict_rate = 100;
        let key_gen = KeyGen::ConflictRate { conflict_rate };
        let payload_size = 100;
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );

        // client
        let id = 1;
        Client::new(id, workload)
    }

    #[test]
    fn discover() {
        // create planet
        let planet = Planet::new();

        // there are two shards
        let shard_0_id = 0;
        let shard_1_id = 1;

        // processes
        let processes = vec![
            (0, shard_0_id, Region::new("asia-east1")),
            (1, shard_0_id, Region::new("australia-southeast1")),
            (2, shard_0_id, Region::new("europe-west1")),
            (3, shard_1_id, Region::new("europe-west2")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 0;
        let mut client = gen_client(total_commands);

        // check discover with empty vec
        let sorted = util::sort_processes_by_distance(&region, &planet, vec![]);
        client.discover(sorted);
        assert!(client.connected.is_empty());

        // check discover with processes
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        dbg!(&sorted);
        client.discover(sorted);
        assert_eq!(
            BTreeMap::from_iter(client.connected),
            // connected to process 2 on shard 0 and process 3 on shard 1
            BTreeMap::from_iter(vec![(shard_0_id, 2), (shard_1_id, 3)])
        );
    }

    #[test]
    fn client_flow() {
        // create planet
        let planet = Planet::new();

        // there's a single shard
        let shard_id = 0;

        // processes
        let processes = vec![
            (0, shard_id, Region::new("asia-east1")),
            (1, shard_id, Region::new("australia-southeast1")),
            (2, shard_id, Region::new("europe-west1")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 2;
        let mut client = gen_client(total_commands);

        // discover
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        client.discover(sorted);

        // create system time
        let mut time = SimTime::new();

        // creates a fake command result from a command
        let fake_result =
            |cmd: Command| vec![CommandResult::new(cmd.rifl(), 0)];

        // start client at time 0
        let (process_id, cmd) = client
            .next_cmd(&time)
            .expect("there should a first operation");
        // process_id should be 2
        assert_eq!(process_id, 2);

        // handle result at time 10
        time.add_millis(10);
        client.handle(fake_result(cmd), &time);
        let next = client.next_cmd(&time);

        // check there's next command
        assert!(next.is_some());
        let (process_id, cmd) = next.unwrap();
        // process_id should be 2
        assert_eq!(process_id, 2);

        // handle result at time 15
        time.add_millis(5);
        client.handle(fake_result(cmd), &time);
        let next = client.next_cmd(&time);

        // check there's no next command
        assert!(next.is_none());

        // check latency
        let mut latency: Vec<_> = client.data().latency_data().collect();
        latency.sort();
        assert_eq!(
            latency,
            vec![Duration::from_millis(5), Duration::from_millis(10)]
        );

        // check throughput
        let mut throughput: Vec<_> = client.data().throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(10, 1), (15, 1)],);
    }
}
