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
            Protocol::NewtAtomic => "Newt-A",
            Protocol::NewtLocked => "Newt-L",
            Protocol::Basic => "Basic",
        }
    }

    pub fn label(protocol: Protocol, f: usize) -> String {
        format!("{} f = {}", Self::protocol_name(protocol), f)
    }

    pub fn color(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "#1abc9c",
            (Protocol::AtlasLocked, 2) => "#218c74",
            (Protocol::EPaxosLocked, _) => "#227093",
            (Protocol::FPaxos, 1) => "#bdc3c7",
            (Protocol::FPaxos, 2) => "#34495e",
            (Protocol::NewtAtomic, 1) => "#ffa726",
            (Protocol::NewtAtomic, 2) => "#e65100",
            (Protocol::NewtLocked, 1) => "#3498db",
            (Protocol::NewtLocked, 2) => "#2980b9",
            (Protocol::Basic, _) => "",
            _ => panic!(
                "PlotFmt::color: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }
    }

    // Possible values: {'/', '\', '|', '-', '+', 'x', 'o', 'O', '.', '*'}
    pub fn hatch(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "/", // 1
            (Protocol::AtlasLocked, 2) => "\\",
            (Protocol::EPaxosLocked, _) => "///", // 3
            (Protocol::FPaxos, 1) => "//", // 2
            (Protocol::FPaxos, 2) => "\\\\",
            (Protocol::NewtAtomic, 1) => "////", // 4
            (Protocol::NewtAtomic, 2) => "\\\\\\\\",
            (Protocol::NewtLocked, 1) => "/////", // 5
            (Protocol::NewtLocked, 2) => "\\\\\\\\\\",
            (Protocol::Basic, 1) => "//////", // 6 
            (Protocol::Basic, 2) => "\\\\\\\\\\\\",
            _ => panic!(
                "PlotFmt::hatch: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }
    }

    // Possible values: https://matplotlib.org/3.1.1/api/markers_api.html#module-matplotlib.markers
    pub fn marker(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::AtlasLocked, 1) => "s",
            (Protocol::AtlasLocked, 2) => "D",
            (Protocol::EPaxosLocked, _) => ".",
            (Protocol::FPaxos, 1) => "+",
            (Protocol::FPaxos, 2) => "x",
            (Protocol::NewtAtomic, 1) => "v",
            (Protocol::NewtAtomic, 2) => "^",
            (Protocol::NewtLocked, 1) => ">",
            (Protocol::NewtLocked, 2) => "<",
            (Protocol::Basic, 1) => "|",
            (Protocol::Basic, 2) => "_",
            _ => panic!(
                "PlotFmt::marker: protocol = {:?} and f = {} combination not supported!",
                protocol, f
            ),
        }
    }

    // Possible values:  {'-', '--', '-.', ':', ''}
    pub fn linestyle(protocol: Protocol, f: usize) -> &'static str {
        match (protocol, f) {
            (Protocol::AtlasLocked, _) => "--",
            (Protocol::EPaxosLocked, _) => ":",
            (Protocol::FPaxos, _) => "-.",
            (Protocol::NewtAtomic, _) => "-",
            (Protocol::NewtLocked, _) => "-",
            (Protocol::Basic, _) => "",
        }
    }

    pub fn linewidth(f: usize) -> f64 {
        match f {
            1 => 1.5,
            2 => 2.0,
            _ => panic!("PlotFmt::linewidth: f = {} not supported!", f),
        }
    }
}
