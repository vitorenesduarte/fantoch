// This module contains the definition of `Dat`.
mod dat;

// This module contains the definition of `Region`.
pub mod region;

// Re-exports.
pub use region::Region;

use crate::planet::dat::Dat;
use crate::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Write};

// directory that contains all dat files for GCP
const GCP_LAT_DIR: &str = "../latency_gcp/";

// assume that intra region latency is 0
const INTRA_REGION_LATENCY: u64 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Planet {
    /// mapping from region A to a mapping from region B to the latency between
    /// A and B
    latencies: HashMap<Region, HashMap<Region, u64>>,
    /// mapping from each region to the regions sorted by distance
    sorted: HashMap<Region, Vec<(u64, Region)>>,
}

impl Planet {
    /// Creates a new GCP `Planet` instance.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::from(GCP_LAT_DIR)
    }

    /// Creates a new `Planet` instance.
    pub fn from(lat_dir: &str) -> Self {
        // create latencies
        let latencies: HashMap<_, _> = Dat::all_dats(lat_dir)
            .iter()
            .map(|dat| (dat.region(), dat.latencies()))
            .collect();

        // also create sorted
        let sorted = Self::sort_by_distance(latencies.clone());

        // return a new planet
        Planet { latencies, sorted }
    }

    /// Creates a equidistant `Planet`.
    pub fn equidistant(
        planet_distance: u64,
        region_number: usize,
    ) -> (Vec<Region>, Self) {
        // create regions
        let regions: Vec<_> = (0..region_number)
            .map(|i| {
                // region name
                let name = format!("r_{}", i);
                Region::new(name)
            })
            .collect();

        // create latencies
        let latencies: HashMap<_, _> = regions
            .clone()
            .into_iter()
            .map(|from| {
                // create distances
                let distances = regions
                    .clone()
                    .into_iter()
                    .map(|to| {
                        let distance = if from == to {
                            INTRA_REGION_LATENCY
                        } else {
                            planet_distance
                        };
                        (to, distance)
                    })
                    .collect();

                (from, distances)
            })
            .collect();

        // create sorted
        let sorted = Self::sort_by_distance(latencies.clone());

        // create single-region planet
        let planet = Planet { latencies, sorted };
        (regions, planet)
    }

    /// Retrieves a list with all regions.
    pub fn regions(&self) -> Vec<Region> {
        self.latencies.keys().cloned().collect()
    }

    /// Retrieves the distance between the two regions passed as argument.
    pub fn ping_latency(&self, from: &Region, to: &Region) -> Option<u64> {
        // get from's entries
        let entries = self.latencies.get(from)?;

        // get to's entry in from's entries
        entries.get(to).cloned()
    }

    /// Returns a list of `Region`s sorted by the distance to the `Region`
    /// passed as argument. The distance to each region is also returned.
    pub fn sorted(&self, from: &Region) -> Option<&Vec<(u64, Region)>> {
        self.sorted.get(from)
    }

    /// Returns a mapping from region to regions sorted by distance (ASC).
    fn sort_by_distance(
        latencies: HashMap<Region, HashMap<Region, u64>>,
    ) -> HashMap<Region, Vec<(u64, Region)>> {
        latencies
            .into_iter()
            .map(|(from, entries)| {
                // collect entries into a vector with reversed tuple order
                let mut entries: Vec<_> = entries
                    .into_iter()
                    .map(|(to, latency)| (latency, to))
                    .collect();

                // sort entries by latency
                entries.sort_unstable();

                (from, entries)
            })
            .collect()
    }
}

impl Planet {
    pub fn distance_matrix(
        &self,
        regions: Vec<Region>,
    ) -> Result<String, fmt::Error> {
        let mut output = String::new();

        // start header
        write!(&mut output, "| |")?;
        for r in regions.iter() {
            write!(&mut output, " {:?} |", r)?;
        }
        writeln!(&mut output)?;

        // end header
        write!(&mut output, "|:---:|")?;
        for _ in regions.iter() {
            write!(&mut output, ":---:|")?;
        }
        writeln!(&mut output)?;

        // for each region a
        for a in regions.iter() {
            write!(&mut output, "| __{:?}__ |", a)?;

            // compute latency from a to every other region b
            for b in regions.iter() {
                let lat = self.ping_latency(a, b).unwrap();
                write!(&mut output, " {} |", lat)?;
            }
            writeln!(&mut output)?;
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symmetric(a: &Region, b: &Region, planet: &Planet) -> bool {
        let a_to_b = planet.ping_latency(a, b).unwrap();
        let b_to_a = planet.ping_latency(b, a).unwrap();
        a_to_b == b_to_a
    }

    #[test]
    fn latency() {
        // planet
        let planet = Planet::new();

        // regions
        let eu_w3 = Region::new("europe-west3");
        let us_c1 = Region::new("us-central1");

        // most times latency is symmetric in GCP
        assert!(symmetric(&eu_w3, &us_c1, &planet));

        // sometimes it's not
        let us_e1 = Region::new("us-east1");
        let eu_w3 = Region::new("europe-west3");
        let us_e4 = Region::new("us-east4");
        let us_w1 = Region::new("us-west1");
        assert!(!symmetric(&us_e1, &eu_w3, &planet));
        assert!(!symmetric(&us_e4, &us_w1, &planet));
        assert!(!symmetric(&us_w1, &eu_w3, &planet));
    }

    #[test]
    fn sorted() {
        // planet
        let planet = Planet::new();

        // regions
        let eu_w3 = Region::new("europe-west3");

        // create expected regions:
        // - the first two have the same value, so they're ordered by name
        let expected = vec![
            Region::new("europe-west3"),
            Region::new("europe-west4"),
            Region::new("europe-west6"),
            Region::new("europe-west1"),
            Region::new("europe-west2"),
            Region::new("europe-north1"),
            Region::new("us-east4"),
            Region::new("northamerica-northeast1"),
            Region::new("us-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
            Region::new("us-west2"),
            Region::new("southamerica-east1"),
            Region::new("asia-northeast1"),
            Region::new("asia-northeast2"),
            Region::new("asia-east1"),
            Region::new("asia-east2"),
            Region::new("australia-southeast1"),
            Region::new("asia-southeast1"),
            Region::new("asia-south1"),
        ];
        // get sorted regions from `eu_w3`, drop the distance and clone the
        // region
        let res: Vec<_> = planet
            .sorted(&eu_w3)
            .unwrap()
            .into_iter()
            .map(|(_, region)| region)
            .cloned()
            .collect();
        assert_eq!(res, expected);
    }

    #[test]
    fn equidistant() {
        let planet_distance = 10;
        let region_number = 3;
        let (regions, planet) =
            Planet::equidistant(planet_distance, region_number);

        // check correct number of regions
        assert_eq!(regions.len(), region_number);

        // check distances between regions
        for a in regions.iter() {
            for b in regions.iter() {
                let a_to_b =
                    planet.ping_latency(a, b).expect("regions must exist");
                let expected = if a == b {
                    INTRA_REGION_LATENCY
                } else {
                    planet_distance
                };
                assert_eq!(a_to_b, expected);
            }
        }
    }

    #[test]
    fn distance_matrix() {
        let planet = Planet::new();
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
        assert!(planet.distance_matrix(regions).is_ok());
    }
}
