mod common;

use fantoch::protocol::Basic;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    common::protocol::run::<Basic>()
}
