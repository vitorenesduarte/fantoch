mod common;

use fantoch_ps::protocol::EPaxosSequential;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<EPaxosSequential>()
}
