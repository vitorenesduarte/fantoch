mod common;

use color_eyre::Report;
use fantoch_ps::protocol::CaesarLocked;

fn main() -> Result<(), Report> {
    common::protocol::run::<CaesarLocked>()
}
