use crate::planet::Region;
use std::collections::HashMap;
use std::str::FromStr;

use std::io::{BufRead, BufReader};

// TODO
// when we create Dat, we should compute region and latencies in that same
// method; also, we should only assume a structure in the filename (region.dat)
// and not in the folder structure (as we're doing now: latency/region.dat). the
// current implementation of region e.g. only works for that structure

#[derive(Debug)]
pub struct Dat {
    filename: String,
}

impl Dat {
    /// Computes this `Dat`'s region.
    pub fn region(&self) -> Region {
        let region = self
            .filename
            .split(|c| c == '/' || c == '.')
            .nth(1)
            .unwrap();
        Region::new(region)
    }

    /// Computes, based on the `Dat` file, the latency from this region to all
    /// other regions.
    /// The local latency (within the same region) will always be 1.
    pub fn latencies(&self) -> HashMap<Region, usize> {
        // open the file in read-only mode (ignoring errors)
        let file = std::fs::File::open(self.filename.clone()).unwrap();

        // get this region
        let this_region = self.region();

        // for each line in the file, compute a pair (region, latency)
        // and filter out the pair about this `Dat`'s region
        BufReader::new(file)
            .lines()
            .map(|line| line.unwrap())
            .map(|line| Dat::latency(line))
            .map(|(region, latency)| {
                if region == this_region {
                    (region, 1)
                } else {
                    (region, latency)
                }
            })
            .collect()
    }

    /// Extracts from a line of the `Dat` file, the region's name and the
    /// average latency to it.
    fn latency(line: String) -> (Region, usize) {
        let mut iter = line.split(|c| c == '/' || c == ':');

        // latency is in the second entry
        let latency = iter.nth(1).unwrap();
        // convert it to f32
        let latency = f32::from_str(latency).unwrap();
        // convert it to usize (it always rounds down)
        let latency = latency as usize;

        // region is the last entry
        let region = iter.last().unwrap();
        // convert it to Region
        let region = Region::new(region);

        // return both
        (region, latency)
    }

    /// Gets the list of all `Dat`'s present in `LAT_DIR`.
    pub fn all_dats(lat_dir: &str) -> Vec<Dat> {
        // create path and check  it is indeed a dir
        let path = std::path::Path::new(lat_dir);

        // get all .dat files in lat dir
        path.read_dir()
            .expect(format!("read_dir {:?} failed", path).as_str())
            // map all entries to PathBuf
            .map(|entry| entry.unwrap().path())
            // map all entries to &str
            .map(|entry| entry.to_str().unwrap().to_string())
            // get only files that end in ".dat"
            .filter(|entry| entry.ends_with(".dat"))
            // map all entry to Dat
            .map(|entry| Dat::from(entry))
            .collect()
    }
}

impl From<String> for Dat {
    fn from(filename: String) -> Self {
        Dat { filename }
    }
}

impl From<&str> for Dat {
    fn from(filename: &str) -> Self {
        Dat {
            filename: String::from(filename),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dat::Dat;
    use crate::planet::Region;
    use std::collections::HashMap;

    #[test]
    fn region() {
        // create dat
        let filename = "latency/europe-west3.dat";
        let dat = Dat::from(filename);

        assert_eq!(dat.region(), Region::new("europe-west3"));
    }

    #[test]
    fn latencies() {
        // create dat
        let filename = "latency/europe-west3.dat";
        let dat = Dat::from(filename);

        // create expected latencies
        let mut expected = HashMap::new();
        expected.insert(Region::new("europe-west3"), 1);
        expected.insert(Region::new("europe-west4"), 7);
        expected.insert(Region::new("europe-west1"), 7);
        expected.insert(Region::new("europe-west2"), 13);
        expected.insert(Region::new("europe-north1"), 31);
        expected.insert(Region::new("us-east4"), 86);
        expected.insert(Region::new("northamerica-northeast1"), 92);
        expected.insert(Region::new("us-east1"), 97);
        expected.insert(Region::new("us-central1"), 109);
        expected.insert(Region::new("us-west1"), 143);
        expected.insert(Region::new("us-west2"), 145);
        expected.insert(Region::new("southamerica-east1"), 214);
        expected.insert(Region::new("asia-northeast1"), 231);
        expected.insert(Region::new("asia-east1"), 261);
        expected.insert(Region::new("australia-southeast1"), 281);
        expected.insert(Region::new("asia-southeast1"), 293);
        expected.insert(Region::new("asia-south1"), 355);

        assert_eq!(dat.latencies(), expected);
    }
}
