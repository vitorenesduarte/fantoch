mod common;

use fantoch_ps::protocol::NewtFineLocked;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<NewtFineLocked>()
}
