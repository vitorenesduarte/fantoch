mod compress;
mod dstat;
mod exp_data;
mod results_db;

// Re-exports.
pub use compress::{DstatCompress, HistogramCompress};
pub use dstat::Dstat;
pub use exp_data::ExperimentData;
pub use results_db::ResultsDB;

use fantoch::client::{KeyGen, ShardGen};
use fantoch_exp::Protocol;

#[derive(Clone, Copy)]
pub struct Search {
    pub n: usize,
    pub f: usize,
    pub protocol: Protocol,
    pub clients_per_region: Option<usize>,
    pub shards_per_command: Option<usize>,
    pub shard_gen: Option<ShardGen>,
    pub keys_per_shard: Option<usize>,
    pub key_gen: Option<KeyGen>,
    pub payload_size: Option<usize>,
}

impl Search {
    pub fn new(n: usize, f: usize, protocol: Protocol) -> Self {
        Self {
            n,
            f,
            protocol,
            clients_per_region: None,
            shards_per_command: None,
            shard_gen: None,
            keys_per_shard: None,
            key_gen: None,
            payload_size: None,
        }
    }

    pub fn clients_per_region(
        &mut self,
        clients_per_region: usize,
    ) -> &mut Self {
        self.clients_per_region = Some(clients_per_region);
        self
    }

    pub fn shards_per_command(
        &mut self,
        shards_per_command: usize,
    ) -> &mut Self {
        self.shards_per_command = Some(shards_per_command);
        self
    }

    pub fn shard_gen(&mut self, key_gen: ShardGen) -> &mut Self {
        self.shard_gen = Some(key_gen);
        self
    }

    pub fn keys_per_shard(&mut self, keys_per_shard: usize) -> &mut Self {
        self.keys_per_shard = Some(keys_per_shard);
        self
    }

    pub fn key_gen(&mut self, key_gen: KeyGen) -> &mut Self {
        self.key_gen = Some(key_gen);
        self
    }

    pub fn payload_size(&mut self, payload_size: usize) -> &mut Self {
        self.payload_size = Some(payload_size);
        self
    }
}
