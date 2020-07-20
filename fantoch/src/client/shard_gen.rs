use crate::id::ShardId;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ShardGen {
    Random { shards: usize },
}

impl ShardGen {
    pub fn gen_shard(&self) -> ShardId {
        match self {
            Self::Random { shards } => Self::gen_random(*shards),
        }
    }

    fn gen_random(shards: usize) -> ShardId {
        rand::thread_rng().gen_range(0, shards) as ShardId
    }
}
