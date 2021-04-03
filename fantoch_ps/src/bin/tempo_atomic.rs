mod common;

use color_eyre::Report;
use fantoch_ps::protocol::TempoAtomic;

fn main() -> Result<(), Report> {
    common::protocol::run::<TempoAtomic>()
}
