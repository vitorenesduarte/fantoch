use fantoch::planet::{Planet, Region};
use fantoch_bote::{FTMetric, RankingParams, Search, SearchInput};

fn main() {
    distance_table();
    search();
}

fn distance_table() {
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
    if let Ok(matrix) = planet.distance_matrix(regions) {
        println!("{}", matrix);
    }
}

fn search() {
    // define some search params
    let min_n = 3;
    let max_n = 13;
    // originally `search_input = SearchInput::R17CMaxN`
    let search_input = SearchInput::R13C13;
    let save_search = true;

    // create search
    let search = Search::new(min_n, max_n, search_input, save_search, None);

    // define search params:
    // originally 30 was used for the `min_mean_improv`;
    // here we want the test to be run asap,
    // so we restrict the search the maximum possible
    let min_mean_fpaxos_improv = 110;
    let min_mean_epaxos_improv = 35;
    let min_fairness_fpaxos_improv = 0;
    let min_mean_decrease = 15;
    let ft_metric = FTMetric::F1F2;

    // create ranking params
    let params = RankingParams::new(
        min_mean_fpaxos_improv,
        min_mean_epaxos_improv,
        min_fairness_fpaxos_improv,
        min_mean_decrease,
        min_n,
        max_n,
        ft_metric,
    );

    // select the best config
    let (score, css, _clients) = search
        .sorted_evolving_configs(&params)
        .into_iter()
        .take(1) // take only the best one
        .next()
        .unwrap();

    println!("score: {:?}", score);

    // the final sorted config
    let mut sorted_config = Vec::new();
    for (config, stats) in css {
        // update sorted config
        for region in config {
            if !sorted_config.contains(region) {
                sorted_config.push(region.clone())
            }
        }

        println!("{}", Search::stats_fmt(stats, config.len()));
    }
}
