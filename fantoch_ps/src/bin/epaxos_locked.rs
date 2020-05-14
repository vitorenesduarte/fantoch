mod common;

use fantoch_ps::protocol::EPaxosLocked;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<EPaxosLocked>()
}
