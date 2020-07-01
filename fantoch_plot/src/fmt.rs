use fantoch::planet::Region;
use fantoch_exp::Protocol;

pub struct PlotFmt;

impl PlotFmt {
    pub fn region_name(region: Region) -> &'static str {
        match region.name().as_str() {
            "ap-southeast-1" => "Singapore",
            "ca-central-1" => "Canada",
            "eu-west-1" => "Ireland",
            "sa-east-1" => "São Paulo",
            "us-west-1" => "N. California", // Northern California
            name => {
                panic!("PlotFmt::region_name: name {} not supported!", name);
            }
        }
    }

    pub fn protocol_name(protocol: Protocol) -> &'static str {
        match protocol {
            Protocol::AtlasLocked => "Atlas",
            Protocol::EPaxosLocked => "EPaxos",
            Protocol::FPaxos => "FPaxos",
            Protocol::NewtAtomic => "Newt",
        }
    }

    pub fn color(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::NewtAtomic, 1) => "#ffa726",
            (Protocol::NewtAtomic, 2) => "#e65100",
            (Protocol::AtlasLocked, 1) => "#1abc9c",
            (Protocol::AtlasLocked, 2) => "#218c74",
            // Atlas in blue:
            // (Protocol::AtlasLocked, 1) => "#3498db",
            // (Protocol::AtlasLocked, 2) => "#2980b9",
            (Protocol::FPaxos, 1) => "#bdc3c7",
            (Protocol::FPaxos, 2) => "#34495e",
            (Protocol::EPaxosLocked, _) => "#227093",
            _ => panic!(
                "PlotFmt::color: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }
    }

    // Possible values: {'/', '\', '|', '-', '+', 'x', 'o', 'O', '.', '*'}
    pub fn hatch(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::NewtAtomic, 1) => "////",
            (Protocol::NewtAtomic, 2) => "\\\\\\\\",
            (Protocol::AtlasLocked, 1) => "///",
            (Protocol::AtlasLocked, 2) => "\\\\\\",
            (Protocol::FPaxos, 1) => "//",
            (Protocol::FPaxos, 2) => "\\\\",
            (Protocol::EPaxosLocked, _) => "/",
            _ => panic!(
                "PlotFmt::hatch: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }
    }
}
