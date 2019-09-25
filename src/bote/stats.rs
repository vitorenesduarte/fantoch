use std::cmp::Ordering;
use std::fmt;

pub struct Stats {
    mean: usize,
    fairness: usize, // mean distance to mean
}

impl fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.mean, self.fairness)
    }
}

impl Stats {
    pub fn from(latencies: &Vec<usize>) -> Self {
        let mean = Stats::compute_mean(latencies);
        let fairness = Stats::compute_mean_distance_to_mean(mean, latencies);
        Stats { mean, fairness }
    }

    pub fn mean(&self) -> usize {
        self.mean
    }

    pub fn fairness(&self) -> usize {
        self.fairness
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

    fn compute_mean_distance_to_mean(mean: usize, xs: &Vec<usize>) -> usize {
        let distances: Vec<usize> = xs
            .into_iter()
            .map(|&x| {
                let distance = (x as isize) - (mean as isize);
                let abs_distance = distance.abs() as usize;
                abs_distance
            })
            .collect();
        Stats::compute_mean(&distances)
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
