mod common;

use color_eyre::Report;
use fantoch_ps::protocol::EPaxosLocked;

fn main() -> Result<(), Report> {
    common::protocol::run::<EPaxosLocked>()
}
