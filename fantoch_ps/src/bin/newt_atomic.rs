mod common;

use fantoch_ps::protocol::NewtAtomic;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<NewtAtomic>()
}
