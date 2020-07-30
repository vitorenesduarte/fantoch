pub mod aws;
pub mod baremetal;
pub mod local;

use crate::config::Placement;
use fantoch::id::{ProcessId, ShardId};
use fantoch::planet::Region;
use std::collections::HashMap;

const NICKNAME_SEP: &str = "_";
const SERVER_TAG: &str = "server";
const CLIENT_TAG: &str = "client";

pub struct Nickname {
    pub region: Region,
    pub shard_id: Option<ShardId>,
}

impl Nickname {
    pub fn new<S: Into<String>>(region: S, shard_id: Option<ShardId>) -> Self {
        let region = Region::new(region);
        Self { region, shard_id }
    }

    pub fn to_string(&self) -> String {
        if let Some(shard_id) = self.shard_id {
            // if there's a shard, then it's server
            format!(
                "{}{}{:?}{}{}",
                SERVER_TAG, NICKNAME_SEP, self.region, NICKNAME_SEP, shard_id
            )
        } else {
            // otherwise it is a client
            format!("{}{}{:?}", CLIENT_TAG, NICKNAME_SEP, self.region)
        }
    }

    pub fn from_string<S: Into<String>>(nickname: S) -> Self {
        let nickname = nickname.into();
        let parts: Vec<_> = nickname.split(NICKNAME_SEP).collect();
        match parts[0] {
            SERVER_TAG => {
                assert_eq!(parts.len(), 3);
                let region = parts[1];
                let shard_id = parts[2]
                    .parse::<ShardId>()
                    .expect("shard id should be a number");
                Self::new(region, Some(shard_id))
            }
            CLIENT_TAG => {
                assert_eq!(parts.len(), 2);
                let region = parts[1];
                Self::new(region, None)
            }
            tag => {
                panic!("found unexpected tag {} in nickname", tag);
            }
        }
    }
}

pub fn create_nicknames(
    shard_count: usize,
    regions: &Vec<rusoto_core::Region>,
) -> Vec<Nickname> {
    // create nicknames for all machines
    let mut nicknames = Vec::new();
    for region in regions.iter() {
        // create servers for this region
        for shard_id in 0..shard_count as ShardId {
            nicknames.push(Nickname::new(region.name(), Some(shard_id)));
        }

        // create client for this region
        nicknames.push(Nickname::new(region.name(), None));
    }
    nicknames
}

/// If shard_count = 3, and regions = [A, B, C, D, E], this function outputs a
/// map with 15 entries:
/// - (A, 0) -> 1
/// - (A, 1) -> 6
/// - (A, 2) -> 11
/// - (B, 0) -> 2
/// - (B, 1) -> 7
/// - (B, 2) -> 12
/// - (C, 0) -> 3
/// - and so on
///
/// Note that the order in the `regions` passed in is respected when generating
/// the process ids.
pub fn create_placement(
    shard_count: usize,
    regions: Vec<rusoto_core::Region>,
) -> Placement {
    let n = regions.len();
    let placement: HashMap<_, _> = regions
        .into_iter()
        .enumerate()
        .flat_map(|(index, region)| {
            let region_index = index + 1;
            (0..shard_count)
                .map(move |shard_id| (region_index, region.clone(), shard_id))
        })
        .map(|(region_index, region, shard_id)| {
            let process_id = region_index + (shard_id * n);
            let region = Region::new(region.name());
            (
                (region, shard_id as ShardId),
                (process_id as ProcessId, region_index),
            )
        })
        .collect();

    // check that we correctly generated processs ids
    let id_to_shard_id: HashMap<_, _> =
        fantoch::util::all_process_ids(shard_count, n).collect();
    for ((_, shard_id), (process_id, _)) in placement.iter() {
        let expected = id_to_shard_id
            .get(process_id)
            .expect("generated process id should exist in all ids");
        assert_eq!(expected, shard_id)
    }

    // return generated placement
    placement
}
