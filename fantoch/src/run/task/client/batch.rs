use crate::command::Command;
use crate::id::{Rifl, ShardId};
use crate::HashMap;
use std::iter::FromIterator;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Batch {
    cmd: Command,
    rifls: Vec<Rifl>,
    deadline: Instant,
    // mapping from shard id to the number of times it was selected as the
    // target for the commands in this batch
    target_shards: HashMap<ShardId, usize>,
}

impl Batch {
    pub fn new(target_shard: ShardId, cmd: Command, deadline: Instant) -> Self {
        let rifl = cmd.rifl();
        Self {
            cmd,
            rifls: vec![rifl],
            deadline,
            target_shards: HashMap::from_iter(vec![(target_shard, 1)]),
        }
    }

    pub fn merge(&mut self, target_shard: ShardId, other: Command) {
        // check that the target shard is one of the shards accessed by the
        // command
        assert!(other.shards().any(|shard_id| shard_id == &target_shard));

        let rifl = other.rifl();
        self.cmd.merge(other);
        // add this command's rifl to the list of rifls in this batch
        self.rifls.push(rifl);
        // update target shard counts
        let current_count = self.target_shards.entry(target_shard).or_default();
        *current_count += 1;
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    #[cfg(test)]
    pub fn rifls(&self) -> &Vec<Rifl> {
        &self.rifls
    }

    pub fn size(&self) -> usize {
        self.rifls.len()
    }

    /// Computes the target shard as the shard most selected as the target
    /// shard. Assume that the batch is non-empty.
    fn target_shard(&self) -> ShardId {
        assert!(self.size() > 0);
        let mut count_to_shard: Vec<_> = self
            .target_shards
            .iter()
            .map(|(shard_id, count)| (count, shard_id))
            .collect();
        // sort by count
        count_to_shard.sort_unstable();
        // return the shard id with the highest count
        *count_to_shard.pop().map(|(_, shard_id)| shard_id).unwrap()
    }

    pub fn unpack(self) -> (ShardId, Command, Vec<Rifl>) {
        let target_shard = self.target_shard();
        (target_shard, self.cmd, self.rifls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvs::{KVOp, Key};

    #[test]
    fn batch_test() {
        let rifl1 = Rifl::new(1, 1);
        let rifl2 = Rifl::new(1, 2);
        let rifl3 = Rifl::new(1, 3);
        let rifl4 = Rifl::new(1, 4);
        let rifl5 = Rifl::new(1, 5);

        let create_command = |rifl: Rifl, shard_id: ShardId, key: Key| {
            let mut shard_ops = HashMap::new();
            shard_ops.insert(key, vec![KVOp::Get]);

            let mut shard_to_ops = HashMap::new();
            shard_to_ops.insert(shard_id, shard_ops);

            Command::new(rifl, shard_to_ops)
        };

        // shard 1
        let shard1: ShardId = 1;
        let key_a = String::from("A");
        let cmd1 = create_command(rifl1, shard1, key_a.clone());
        let cmd2 = create_command(rifl2, shard1, key_a.clone());

        // shard 2
        let shard2: ShardId = 2;
        let key_b = String::from("B");
        let key_c = String::from("C");
        let cmd3 = create_command(rifl3, shard2, key_b.clone());
        let cmd4 = create_command(rifl4, shard2, key_b.clone());
        let cmd5 = create_command(rifl5, shard2, key_c.clone());

        let mut batch = Batch::new(shard1, cmd1, Instant::now());
        assert_eq!(batch.rifls(), &vec![rifl1]);
        assert_eq!(batch.size(), 1);
        assert_eq!(batch.target_shard(), shard1);

        batch.merge(shard1, cmd2);
        assert_eq!(batch.rifls(), &vec![rifl1, rifl2]);
        assert_eq!(batch.size(), 2);
        assert_eq!(batch.target_shard(), shard1);

        batch.merge(shard2, cmd3);
        assert_eq!(batch.rifls(), &vec![rifl1, rifl2, rifl3]);
        assert_eq!(batch.size(), 3);
        assert_eq!(batch.target_shard(), shard1);

        batch.merge(shard2, cmd4);
        assert_eq!(batch.rifls(), &vec![rifl1, rifl2, rifl3, rifl4]);
        assert_eq!(batch.size(), 4);
        // at this point the target shard can be either as both have the same
        // count
        assert!(
            batch.target_shard() == shard1 || batch.target_shard() == shard2
        );

        batch.merge(shard2, cmd5);
        assert_eq!(batch.rifls(), &vec![rifl1, rifl2, rifl3, rifl4, rifl5]);
        assert_eq!(batch.size(), 5);
        assert_eq!(batch.target_shard(), shard2);

        // check that the merge has occurred
        assert_eq!(batch.cmd.shard_count(), 2);
        assert_eq!(batch.cmd.key_count(shard1), 1);
        assert_eq!(batch.cmd.key_count(shard2), 2);
        let shard1_keys: Vec<_> = batch.cmd.keys(shard1).collect();
        assert_eq!(shard1_keys.len(), 1);
        assert!(shard1_keys.contains(&&key_a));
        let shard2_keys: Vec<_> = batch.cmd.keys(shard2).collect();
        assert_eq!(shard2_keys.len(), 2);
        assert!(shard2_keys.contains(&&key_b));
        assert!(shard2_keys.contains(&&key_c));
    }
}
