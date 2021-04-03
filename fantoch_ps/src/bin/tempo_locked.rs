mod common;

use color_eyre::Report;
use fantoch_ps::protocol::TempoLocked;

fn main() -> Result<(), Report> {
    common::protocol::run::<TempoLocked>()
}
