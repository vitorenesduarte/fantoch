mod common;

use color_eyre::Report;
use fantoch::protocol::Basic;

fn main() -> Result<(), Report> {
    common::protocol::run::<Basic>()
}
