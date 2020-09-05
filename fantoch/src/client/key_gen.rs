use crate::id::ClientId;
use crate::kvs::Key;
use rand::distributions::Distribution;
use rand::Rng;
use serde::{Deserialize, Serialize};
use zipf::ZipfDistribution;

pub const CONFLICT_COLOR: &str = "CONFLICT";

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum KeyGen {
    ConflictRate { conflict_rate: usize },
    Zipf { coefficient: f64, key_count: usize },
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
            Self::ConflictRate { conflict_rate } => {
                write!(f, "conflict{}", conflict_rate)
            }
            Self::Zipf { coefficient, .. } => write!(f, "zipf{}", coefficient),
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
            KeyGen::ConflictRate { .. } => None,
            KeyGen::Zipf {
                coefficient,
                key_count,
            } => {
                // compute actual key count
                let key_count = key_count * shard_count;
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
            KeyGen::ConflictRate { conflict_rate } => {
                self.gen_conflict_rate(conflict_rate)
            }
            KeyGen::Zipf { .. } => self.gen_zipf(),
        }
    }

    /// Generate a command key based on the conflict rate provided.
    fn gen_conflict_rate(&self, conflict_rate: usize) -> Key {
        debug_assert!(conflict_rate <= 100);

        // check if we should generate a conflict:
        let should_conflict = match conflict_rate {
            0 => false,
            100 => true,
            c => rand::thread_rng().gen_range(0, 100) < c,
        };

        if should_conflict {
            // single color accessed by all conflicting operations
            CONFLICT_COLOR.to_owned()
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
