use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize)]
pub struct Stats {
    mean: usize,
    fairness: usize, // mean dist to mean
    min_max_dist: usize,
}

impl fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.mean, self.fairness,)
    }
}

impl Stats {
    pub fn from(latencies: &Vec<usize>) -> Self {
        let mean = Stats::compute_mean(latencies);
        let fairness = Stats::compute_mean_dist_to_mean(mean, latencies);
        let min_max_dist = Stats::compute_min_max_dist(latencies);
        Stats {
            mean,
            fairness,
            min_max_dist,
        }
    }

    pub fn mean(&self) -> usize {
        self.mean
    }

    pub fn fairness(&self) -> usize {
        self.fairness
    }

    pub fn min_max_dist(&self) -> usize {
        self.min_max_dist
    }

    pub fn mean_improv(&self, o: &Self) -> isize {
        Self::sub(self.mean, o.mean)
    }

    pub fn fairness_improv(&self, o: &Self) -> isize {
        Self::sub(self.fairness, o.fairness)
    }

    fn sub(a: usize, b: usize) -> isize {
        (a as isize) - (b as isize)
    }

    /// This method can be sort a list of `Stat`s and selecting the one with a
    /// best mean. In case there are more than one `Stat` with the best
    /// mean, it will select the one with the best fairness stat.
    pub fn mean_cmp(&self, b: &Self) -> Ordering {
        match self.mean.cmp(&b.mean()) {
            Ordering::Equal => self.fairness.cmp(&b.fairness()),
            o => o,
        }
    }

    /// This method can be sort a list of `Stat`s and selecting the one with a
    /// best fairness stat. In case there are more than one `Stat` with the best
    /// fairness stat, it will select the one with the best mean.
    pub fn fairness_cmp(&self, b: &Self) -> Ordering {
        match self.fairness.cmp(&b.fairness()) {
            Ordering::Equal => self.fairness.cmp(&b.mean()),
            o => o,
        }
    }

    fn compute_mean(xs: &Vec<usize>) -> usize {
        let count = xs.len();
        let sum: usize = xs.into_iter().sum();
        sum / count as usize
    }

    fn compute_mean_dist_to_mean(mean: usize, xs: &Vec<usize>) -> usize {
        let dists: Vec<usize> = xs
            .into_iter()
            .map(|&x| {
                let dist = (x as isize) - (mean as isize);
                let abs_dist = dist.abs() as usize;
                abs_dist
            })
            .collect();
        Stats::compute_mean(&dists)
    }

    fn compute_min_max_dist(xs: &Vec<usize>) -> usize {
        let mut min = usize::max_value();
        let mut max = usize::min_value();
        xs.into_iter().for_each(|&x| {
            if x < min {
                min = x;
            }

            if x > max {
                max = x;
            }
        });
        max - min
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn stats() {
        let stats = Stats::from(&vec![1, 1, 1]);
        assert_eq!(stats.mean(), 1);
        assert_eq!(stats.fairness(), 0);

        let stats = Stats::from(&vec![10, 20, 30]);
        assert_eq!(stats.mean(), 20);
        assert_eq!(stats.fairness(), 6);

        let stats = Stats::from(&vec![10, 20]);
        assert_eq!(stats.mean(), 15);
        assert_eq!(stats.fairness(), 5);

        let stats = Stats::from(&vec![10, 20, 40, 10]);
        assert_eq!(stats.mean(), 20);
        assert_eq!(stats.fairness(), 10);
    }
}
