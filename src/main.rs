use planet_sim::bote::Bote;
use planet_sim::planet::{Planet, Region};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    let planet = Planet::new(LAT_DIR);
    let clients = vec![
        Region::new("us-west2"),
        Region::new("us-east1"),
        Region::new("northamerica-northeast1"),
        Region::new("southamerica-east1"),
        Region::new("europe-west2"),
        Region::new("europe-north1"),
        Region::new("asia-south1"),
        Region::new("asia-southeast1"),
        Region::new("asia-east1"),
        Region::new("asia-east2"),
        Region::new("asia-northeast1"),
    ];
    planet.show_distance_matrix(clients.clone());
    let bote = Bote::from(planet);
    bote.run();
}
