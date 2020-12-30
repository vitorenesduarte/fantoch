mod common;

use color_eyre::Report;
use fantoch_ps::protocol::CaesarSequential;

fn main() -> Result<(), Report> {
    common::protocol::run::<CaesarSequential>()
}
