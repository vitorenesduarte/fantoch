mod common;

use color_eyre::Report;
use fantoch_ps::protocol::NewtAtomic;

fn main() -> Result<(), Report> {
    common::protocol::run::<NewtAtomic>()
}
