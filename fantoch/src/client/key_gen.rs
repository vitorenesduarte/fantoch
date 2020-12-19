use crate::id::ClientId;
use crate::kvs::Key;
use rand::distributions::Distribution;
use rand::Rng;
use serde::{Deserialize, Serialize};
use zipf::ZipfDistribution;

pub const CONFLICT_COLOR: &str = "CONFLICT";

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum KeyGen {
    ConflictPool {
        conflict_rate: usize,
        pool_size: usize,
    },
    Zipf {
        coefficient: f64,
        total_keys_per_shard: usize,
    },
}

impl KeyGen {
    pub fn initial_state(
        self,
        shard_count: usize,
        client_id: ClientId,
    ) -> KeyGenState {
        KeyGenState::new(self, shard_count, client_id)
    }
}

impl std::fmt::Display for KeyGen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictPool {
                conflict_rate,
                pool_size,
            } => {
                write!(f, "conflict_{}_{}", conflict_rate, pool_size)
            }
            Self::Zipf {
                total_keys_per_shard,
                coefficient,
            } => write!(
                f,
                "{}",
                format!("zipf_{:.2}_{}", coefficient, total_keys_per_shard)
                    .replace(".", "-")
            ),
        }
    }
}

pub struct KeyGenState {
    key_gen: KeyGen,
    client_id: ClientId,
    zipf: Option<ZipfDistribution>,
}

impl KeyGenState {
    fn new(key_gen: KeyGen, shard_count: usize, client_id: ClientId) -> Self {
        let zipf = match key_gen {
            KeyGen::ConflictPool { .. } => None,
            KeyGen::Zipf {
                coefficient,
                total_keys_per_shard,
            } => {
                // compute key count
                let key_count = total_keys_per_shard * shard_count;
                // initialize zipf distribution
                let zipf = ZipfDistribution::new(key_count, coefficient)
                    .expect(
                    "it should be possible to initialize the ZipfDistribution",
                );
                Some(zipf)
            }
        };
        Self {
            key_gen,
            client_id,
            zipf,
        }
    }

    pub fn gen_cmd_key(&mut self) -> Key {
        match self.key_gen {
            KeyGen::ConflictPool {
                conflict_rate,
                pool_size,
            } => self.gen_conflict_rate(conflict_rate, pool_size),
            KeyGen::Zipf { .. } => self.gen_zipf(),
        }
    }

    /// Generate a command key based on the conflict rate provided.
    fn gen_conflict_rate(&self, conflict_rate: usize, pool_size: usize) -> Key {
        debug_assert!(conflict_rate <= 100);
        debug_assert!(pool_size >= 1);

        // check if we should generate a conflict
        let should_conflict = true_if_random_is_less_than(conflict_rate);
        if should_conflict {
            // if it should conflict, select a random key from the pool
            let random_key = rand::thread_rng().gen_range(0, pool_size);
            format!("{}{}", CONFLICT_COLOR, random_key)
        } else {
            // avoid conflict with unique client key
            self.client_id.to_string()
        }
    }

    /// Generate a command key based on the initiliazed zipfian distribution.
    fn gen_zipf(&mut self) -> Key {
        let zipf = self
            .zipf
            .expect("ZipfDistribution should already be initialized");
        let mut rng = rand::thread_rng();
        zipf.sample(&mut rng).to_string()
    }
}

pub fn true_if_random_is_less_than(percentage: usize) -> bool {
    match percentage {
        0 => false,
        100 => true,
        _ => rand::thread_rng().gen_range(0, 100) < percentage,
    }
}
