mod common;

use color_eyre::Report;
use fantoch_ps::protocol::FPaxos;

fn main() -> Result<(), Report> {
    common::protocol::run::<FPaxos>()
}
