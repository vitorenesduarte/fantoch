mod common;

use color_eyre::Report;
use fantoch_ps::protocol::NewtFineLocked;

fn main() -> Result<(), Report> {
    common::protocol::run::<NewtFineLocked>()
}
