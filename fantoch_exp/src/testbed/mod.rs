pub mod aws;
pub mod baremetal;

const NICKNAME_SEP: &str = "_";
const SERVER_TAG: &str = "server";
const CLIENT_TAG: &str = "client";

pub fn to_nickname(tag: &str, region: &str) -> String {
    format!("{}{}{}", tag, NICKNAME_SEP, region)
}

pub fn from_nickname(nickname: String) -> (String, fantoch::planet::Region) {
    let parts: Vec<_> = nickname.split(NICKNAME_SEP).collect();
    assert_eq!(parts.len(), 2);
    (parts[0].to_string(), fantoch::planet::Region::new(parts[1]))
}

pub fn to_regions(
    regions: Vec<rusoto_core::Region>,
) -> Vec<fantoch::planet::Region> {
    regions
        .into_iter()
        .map(|region| {
            let region_name = region.name();
            // create fantoch region
            fantoch::planet::Region::new(region_name)
        })
        .collect()
}
