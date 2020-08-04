mod common;

use color_eyre::Report;
use fantoch_ps::protocol::NewtSequential;

fn main() -> Result<(), Report> {
    common::protocol::run::<NewtSequential>()
}
