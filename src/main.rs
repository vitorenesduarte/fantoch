use planet_sim::bote::Bote;

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    let bote = Bote::new(LAT_DIR);
    bote.run();
}
