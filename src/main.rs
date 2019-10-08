use planet_sim::bote::search::{
    RankingFT, RankingMetric, RankingParams, Search, SearchInput,
};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define some search params
    let min_n = 3;
    let max_n = 13;
    let search_input = SearchInput::R17C17;

    // create search
    let search = Search::new(min_n, max_n, search_input, LAT_DIR);
    println!("> search created!");

    panic!("a");

    // define search params
    let min_n = 3;
    let max_n = 13;
    let min_lat_improv = -200;
    let min_fair_improv = 0;
    let max_lat = 550; // 8-550
    let max_fair = 154; // 0-154
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

    println!("> showing best configs: min_lat_improv={}", min_lat_improv);
    let max_configs_per_n = 1;
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
