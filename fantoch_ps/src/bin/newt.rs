mod common;

use fantoch_ps::protocol::NewtSequential;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<NewtSequential>()
}