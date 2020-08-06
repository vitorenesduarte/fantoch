mod common;

use color_eyre::Report;
use fantoch_ps::protocol::AtlasLocked;

// TODO can we generate all the protocol binaries with a macro?

fn main() -> Result<(), Report> {
    common::protocol::run::<AtlasLocked>()
}
