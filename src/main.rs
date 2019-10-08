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

    // define search params
    let min_n = 3;
    let max_n = 13;
    let min_lat_improv = -10;
    let min_fair_improv = 0;
    let ranking_metric = RankingMetric::LatencyAndFairness;
    let ranking_ft = RankingFT::F1F2;

    let params = RankingParams::new(
        min_lat_improv,
        min_fair_improv,
        min_n,
        max_n,
        ranking_metric,
        ranking_ft,
    );

    // println!("> showing best configs: min_lat_improv={}", min_lat_improv);
    // let max_configs_per_n = 1;
    // search
    //     .sorted_configs(&params, max_configs_per_n)
    //     .into_iter()
    //     .for_each(|(n, sorted)| {
    //         println!("n={}", n);
    //         sorted.into_iter().for_each(|(score, (config, stats))| {
    //             println!("{}: {:?}", score, config);
    //             println!("{}", Search::stats_fmt(stats, n));
    //             println!("");
    //         });
    //     });

    println!("> showing evolving configs");
    let max_configs = 100;
    search
        .sorted_evolved_configs(&params, max_configs)
        .into_iter()
        .for_each(|(score, css)| {
            let mut sorted_config = Vec::new();
            let mut all_stats = Vec::new();

            for (config, stats) in css {
                // update sorted config
                for region in config {
                    if !sorted_config.contains(&region) {
                        sorted_config.push(region)
                    }
                }

                // save stats
                let n = config.len();
                all_stats.push((n, stats));
            }

            println!("{}: {:?}", score, sorted_config);

            for (n, stats) in all_stats {
                print!("[n={}] ", n);
                print!("{}", Search::stats_fmt(stats, n));
                print!(" | ");
            }
            println!("");
        });
}
