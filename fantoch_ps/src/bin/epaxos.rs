mod common;

use color_eyre::Report;
use fantoch_ps::protocol::EPaxosSequential;

fn main() -> Result<(), Report> {
    common::protocol::run::<EPaxosSequential>()
}
