mod common;

use fantoch_ps::protocol::NewtLocked;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<NewtLocked>()
}
