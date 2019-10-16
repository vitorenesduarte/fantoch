use planet_sim::bote::search::{
    FTMetric, FairnessMetric, RankingParams, Search, SearchInput,
};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define some search params
    let min_n = 3;
    let max_n = 11;
    let search_input = SearchInput::R17CMaxN;

    // create search
    let search = Search::new(min_n, max_n, search_input, LAT_DIR);
    println!("> search created!");

    // define search params
    let min_mean_improv = 100;
    let min_fairness_improv = 0;
    let min_mean_decrease = 30;
    let fairness_metric = FairnessMetric::COV;
    let ft_metric = FTMetric::F1F2;

    let params = RankingParams::new(
        min_mean_improv,
        min_fairness_improv,
        min_mean_decrease,
        min_n,
        max_n,
        fairness_metric,
        ft_metric,
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
    //             println!("{}", Search::stats_fmt(stats, n, true));
    //             println!("");
    //         });
    //     });

    println!("> showing evolving configs");
    let max_configs = 2;
    search
        .sorted_evolving_configs(&params)
        .into_iter()
        .take(max_configs)
        .for_each(|(score, css, clients)| {
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

            println!("{}:", score.round());
            println!("servers: {:?}", sorted_config);
            println!("clients: {:?}", clients);

            for (n, stats) in all_stats {
                print!("[n={}] ", n);
                print!("{}", Search::stats_fmt(stats, n, &params));
                print!(" | ");
            }
            println!("");
        });
}
