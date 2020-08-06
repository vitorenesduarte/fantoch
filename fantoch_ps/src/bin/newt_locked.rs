mod common;

use color_eyre::Report;
use fantoch_ps::protocol::NewtLocked;

fn main() -> Result<(), Report> {
    common::protocol::run::<NewtLocked>()
}
