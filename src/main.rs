use planet_sim::bote::search::{
    Search, SearchFTFilter, SearchInput, SearchMetric,
};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define search params
    // let min_lat_improv = -500;
    // let min_fairness_improv = -500;
    let min_lat_improv = 20;
    let min_fairness_improv = 0;
    let min_n = 3;
    let max_n = 13;
    let search_metric = SearchMetric::Latency;
    let search_ft_filter = SearchFTFilter::F1F2F3;
    let search_input = SearchInput::R20C20;

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

    println!("> showing evolving configs");
    search.evolving_configs();

    // println!("> showing best configs");
    // let max_configs_per_n = 2;
    // search.best_configs(max_configs_per_n);
}
