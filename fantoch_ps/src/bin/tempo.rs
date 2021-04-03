mod common;

use color_eyre::Report;
use fantoch_ps::protocol::TempoSequential;

fn main() -> Result<(), Report> {
    common::protocol::run::<TempoSequential>()
}
