use crate::id::ShardId;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ShardGen {
    Random { shard_count: usize },
}

impl ShardGen {
    pub fn gen_shard(&self) -> ShardId {
        match self {
            Self::Random { shard_count } => Self::gen_random(*shard_count),
        }
    }

    fn gen_random(shard_count: usize) -> ShardId {
        rand::thread_rng().gen_range(0, shard_count) as ShardId
    }
}
