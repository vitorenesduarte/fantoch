use planet_sim::planet::{Planet, Region};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    let planet = Planet::new(LAT_DIR);
    let regions = vec![
        Region::new("asia-southeast1"),
        Region::new("europe-west4"),
        Region::new("southamerica-east1"),
        Region::new("australia-southeast1"),
        Region::new("europe-west2"),
        Region::new("asia-south1"),
        Region::new("us-east1"),
        Region::new("asia-northeast1"),
        Region::new("europe-west1"),
        Region::new("asia-east1"),
        Region::new("us-west1"),
        Region::new("europe-west3"),
        Region::new("us-central1"),
    ];
    println!("{}", planet.distance_matrix(regions).unwrap());
}
