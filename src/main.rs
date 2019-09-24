use planet_sim::bote::Bote;
use planet_sim::planet::Planet;

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    let planet = Planet::new(LAT_DIR);
    let bote = Bote::new(planet);
    bote.run();
}
