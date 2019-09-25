use planet_sim::bote::Bote;
use planet_sim::planet::{Planet, Region};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // create planet
    let planet = Planet::new(LAT_DIR);

    // compute all regions
    let all_regions = planet.regions();
    println!("all regions: {:?}", all_regions);

    // compute all clients
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

    // create bote
    let bote = Bote::from(planet);

    let stats = bote.leaderless(all_regions, clients, 2);
    println!("{:?}", stats);
}
