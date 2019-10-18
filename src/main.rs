use planet_sim::bote::search::{FTMetric, RankingParams, Search, SearchInput};

// directory that contains all dat files
const LAT_DIR: &str = "latency/";

fn main() {
    // define some search params
    let min_n = 3;
    let max_n = 13;

    // create search
    let search_input = SearchInput::R17CMaxN;
    let search = Search::new(min_n, max_n, search_input, LAT_DIR);
    println!("> search created!");

    // define search params
    let min_mean_improv = 30;
    let min_fairness_improv = 0;
    let min_mean_decrease = 15;
    let ft_metric = FTMetric::F1F2;

    let params = RankingParams::new(
        min_mean_improv,
        min_fairness_improv,
        min_mean_decrease,
        min_n,
        max_n,
        ft_metric,
    );

    println!("> computing evolving configs");
    let max_configs = 2;
    search
        .sorted_evolving_configs(&params)
        .into_iter()
        .take(max_configs)
        .for_each(|(score, css, _clients)| {
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

            println!("{}: {:?}", score.round(), sorted_config);

            for (n, stats) in all_stats {
                print!("[n={}] ", n);
                print!("{}", Search::stats_fmt(stats, n));
                print!("| ");
            }
            println!("");
        });
}
