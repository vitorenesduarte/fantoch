use crate::command::{Command, CommandResult};
use crate::hash_map::{Entry, HashMap};
use crate::id::Rifl;
use crate::trace;

struct Expected {
    shard_count: usize,
    total_key_count: usize,
}

pub struct ShardsPending {
    pending: HashMap<Rifl, (Expected, Vec<CommandResult>)>,
}

impl ShardsPending {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    pub fn register(&mut self, cmd: &Command) {
        let rifl = cmd.rifl();
        trace!("c{}: register {:?}", rifl.source(), rifl);
        let expected = Expected {
            shard_count: cmd.shard_count(),
            total_key_count: cmd.total_key_count(),
        };
        let results = Vec::with_capacity(expected.shard_count);
        let res = self.pending.insert(rifl, (expected, results));
        assert!(res.is_none());
    }

    // Add new `CommandResult`. Return a `Rifl` if some command got the
    // `CommandResult`s from each of the shards accessed.
    pub fn add(&mut self, result: CommandResult) -> Option<Rifl> {
        let rifl = result.rifl();
        trace!("c{}: received {:?}", rifl.source(), rifl);
        match self.pending.entry(rifl) {
            Entry::Occupied(mut entry) => {
                let (expected, results) = entry.get_mut();
                // add new result
                results.push(result);

                trace!(
                    "c{}: {:?} {}/{}",
                    rifl.source(),
                    rifl,
                    results.len(),
                    expected.shard_count
                );

                // return results if we have one `CommandResult` per shard
                // - TODO: add an assert checking that indeed these
                //   `CommandResult` came from different shards, and are not
                //   sent by the same shard
                if results.len() == expected.shard_count {
                    // assert that all keys accessed got a result
                    let results_key_count: usize = results
                        .into_iter()
                        .map(|cmd_result| cmd_result.results().len())
                        .sum();
                    assert_eq!(results_key_count, expected.total_key_count);
                    entry.remove();
                    Some(rifl)
                } else {
                    None
                }
            }
            Entry::Vacant(_) => panic!(
                "received command result about a rifl we didn't register for"
            ),
        }
    }
}
