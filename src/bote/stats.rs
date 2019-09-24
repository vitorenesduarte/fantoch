pub struct Stats {
    mean: usize,
    mean_distance_to_mean: usize,
}

impl Stats {
    pub fn from(xs: &Vec<usize>) -> Self {
        let mean = Stats::compute_mean(xs);
        let mean_distance_to_mean =
            Stats::compute_mean_distance_to_mean(mean, xs);
        Stats {
            mean,
            mean_distance_to_mean,
        }
    }

    pub fn mean(&self) -> usize {
        self.mean
    }

    pub fn mean_distance_to_mean(&self) -> usize {
        self.mean_distance_to_mean
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
        assert_eq!(stats.mean_distance_to_mean(), 0);

        let stats = Stats::from(&vec![10, 20, 30]);
        assert_eq!(stats.mean(), 20);
        assert_eq!(stats.mean_distance_to_mean(), 6);

        let stats = Stats::from(&vec![10, 20]);
        assert_eq!(stats.mean(), 15);
        assert_eq!(stats.mean_distance_to_mean(), 5);

        let stats = Stats::from(&vec![10, 20, 40, 10]);
        assert_eq!(stats.mean(), 20);
        assert_eq!(stats.mean_distance_to_mean(), 10);
    }
}
