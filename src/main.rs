use planet_sim::bote::search::{
    Search, SearchFTFilter, SearchInput, SearchMetric,
};

// directory that contains all dat files
const MIN_LAT_IMPROV: isize = 0;
const MIN_FAIRNESS_IMPROV: isize = 0;
const MAX_N: usize = 11;
const SEARCH_METRIC: SearchMetric = SearchMetric::Latency;
const SEARCH_FT_FILTER: SearchFTFilter = SearchFTFilter::F1AndF2;
const SEARCH_INPUT: SearchInput = SearchInput::C20R20;
const LAT_DIR: &str = "latency/";

fn main() {
    let search = Search::new(
        MIN_LAT_IMPROV,
        MIN_FAIRNESS_IMPROV,
        MAX_N,
        SEARCH_METRIC,
        SEARCH_FT_FILTER,
        SEARCH_INPUT,
        LAT_DIR,
    );
    search.evolving_configs();
}
