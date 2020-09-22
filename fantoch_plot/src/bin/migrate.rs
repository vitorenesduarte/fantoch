use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch::client::Workload;
use fantoch::config::Config;
use fantoch::planet::Planet;
use fantoch_exp::{
    ExperimentConfig, FantochFeature, PlacementFlat, Protocol, RunMode,
    SerializationFormat, Testbed,
};
use fantoch_plot::ResultsDB;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct PreviousExperimentConfig {
    pub placement: PlacementFlat,
    pub planet: Option<Planet>,
    pub run_mode: RunMode,
    pub features: Vec<FantochFeature>,
    pub testbed: Testbed,
    pub protocol: Protocol,
    pub config: Config,
    pub clients_per_region: usize,
    pub workload: Workload,
    pub process_tcp_nodelay: bool,
    pub tcp_buffer_size: usize,
    pub tcp_flush_interval: Option<usize>,
    pub process_channel_buffer_size: usize,
    pub workers: usize,
    pub executors: usize,
    pub multiplexing: usize,
    pub client_tcp_nodelay: bool,
    pub client_channel_buffer_size: usize,
}

fn main() -> Result<(), Report> {
    for results_dir in vec![
        "../results_multi_key",
        "../results_single_key",
        "../results_partial_replication",
    ] {
        // load results
        let timestamps =
            ResultsDB::list_timestamps(results_dir).wrap_err("load results")?;

        for timestamp in timestamps {
            // read the configuration of this experiment
            let exp_config_path =
                format!("{}/exp_config.json", timestamp.path().display());
            let previous: Result<PreviousExperimentConfig, _> =
                fantoch_exp::deserialize(
                    &exp_config_path,
                    SerializationFormat::Json,
                )
                .wrap_err_with(|| {
                    format!(
                        "deserialize experiment config of {:?}",
                        timestamp.path().display()
                    )
                });

            match previous {
                Ok(previous) => {
                    let exp_config = ExperimentConfig {
                        placement: previous.placement,
                        planet: previous.planet,
                        run_mode: previous.run_mode,
                        features: previous.features,
                        testbed: previous.testbed,
                        protocol: previous.protocol,
                        config: previous.config,
                        clients_per_region: previous.clients_per_region,
                        process_tcp_nodelay: previous.process_tcp_nodelay,
                        tcp_buffer_size: previous.tcp_buffer_size,
                        tcp_flush_interval: previous.tcp_flush_interval,
                        process_channel_buffer_size: previous
                            .process_channel_buffer_size,
                        workers: previous.workers,
                        executors: previous.executors,
                        multiplexing: previous.multiplexing,
                        workload: previous.workload,
                        client_tcp_nodelay: previous.client_tcp_nodelay,
                        client_channel_buffer_size: previous
                            .client_channel_buffer_size,
                        cpus: 12,
                    };

                    // save experiment config
                    fantoch_exp::serialize(
                        exp_config,
                        &exp_config_path,
                        SerializationFormat::Json,
                    )
                    .wrap_err("migrate_exp_config")?;
                }
                Err(e) => {
                    let missing_file =
                        String::from("No such file or directory (os error 2)");
                    if e.root_cause().to_string() == missing_file {
                        // if some file was not found, it may be because the
                        // folder is empty; in this case, ignore the
                        // error
                        println!("entry ignored...");
                    } else {
                        // if not, quit
                        return Err(e);
                    }
                }
            }
        }
    }

    Ok(())
}
