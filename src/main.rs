use planet_sim::bote::search::{
    Search, SearchFTFilter, SearchInput, SearchMetric,
};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define search params
    let min_lat_improv = -200;
    let min_fairness_improv = -200;
    let min_n = 3;
    let max_n = 11;
    let search_metric = SearchMetric::LatencyAndFairness;
    let search_ft_filter = SearchFTFilter::F1AndF2;
    let search_input = SearchInput::C11R11;

    // create search
    println!("> creating search");
    let search = Search::new(
        min_lat_improv,
        min_fairness_improv,
        min_n,
        max_n,
        search_metric,
        search_ft_filter,
        search_input,
        LAT_DIR,
    );

    // println!("> showing evolving configs");
    // search.evolving_configs();

    println!("> showing best configs");
    let max_configs_per_n = 10;
    search.best_configs(max_configs_per_n);
}
