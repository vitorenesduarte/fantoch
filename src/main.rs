use planet_sim::bote::search::{
    RankingFT, RankingMetric, RankingParams, Search, SearchInput,
};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define some search params
    let min_n = 3;
    let max_n = 13;
    let search_input = SearchInput::R20;

    // create search
    println!("> creating search");
    let search = Search::new(min_n, max_n, search_input, LAT_DIR);

    // define more search params
    let min_lat_improv = -100;
    let min_fair_improv = 0;
    let max_lat = 500;
    let max_fair = 150;
    let ranking_metric = RankingMetric::LatencyAndFairness;
    let ranking_ft = RankingFT::F1F2;

    let params = RankingParams::new(
        min_lat_improv,
        min_fair_improv,
        max_lat,
        max_fair,
        min_n,
        max_n,
        ranking_metric,
        ranking_ft,
    );

    println!("> showing best configs");
    let max_configs_per_n = 2;
    search
        .sorted_configs(&params, max_configs_per_n)
        .into_iter()
        .for_each(|(n, sorted)| {
            println!("n = {}", n);
            sorted.into_iter().for_each(|(score, (config, stats))| {
                println!("{}: {:?}", score, config);
                println!("{}", Search::stats_fmt(stats, n));
                print!("\n");
            });
        });
}
