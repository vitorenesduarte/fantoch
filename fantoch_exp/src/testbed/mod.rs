pub mod aws;
pub mod baremetal;

use rusoto_core::Region;

const NICKNAME_SEP: &str = "_";
const SERVER_TAG: &str = "server";
const CLIENT_TAG: &str = "client";

pub fn to_nickname(tag: &str, region: &str) -> String {
    format!("{}{}{}", tag, NICKNAME_SEP, region)
}

pub fn from_nickname(nickname: String) -> (String, String) {
    let parts: Vec<_> = nickname.split(NICKNAME_SEP).collect();
    assert_eq!(parts.len(), 2);
    (parts[0].to_string(), parts[1].to_string())
}

pub fn regions(regions: Vec<Region>) -> Vec<String> {
    regions
        .into_iter()
        .map(|region| region.name().to_string())
        .collect()
}
