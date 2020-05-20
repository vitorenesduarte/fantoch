mod common;

use fantoch_ps::protocol::FPaxos;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<FPaxos>()
}
