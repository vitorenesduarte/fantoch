use crate::db::dstat::Dstat;
use crate::db::exp_data::ExperimentData;
use crate::Search;
use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch::client::ClientData;
use fantoch::planet::Region;
use fantoch::run::task::metrics_logger::ProcessMetrics;
use fantoch_exp::{ExperimentConfig, SerializationFormat};
use std::collections::HashMap;
use std::fs::DirEntry;

#[derive(Debug)]
pub struct ResultsDB {
    results: Vec<(DirEntry, ExperimentConfig, Option<ExperimentData>)>,
}

impl ResultsDB {
    /// This is a lazy load. Only configurations are loaded. Then, if any search
    /// matches a given configuration, the full experiment data is loaded from
    /// disk.
    pub fn load(results_dir: &str) -> Result<Self, Report> {
        let mut results = Vec::new();

        for timestamp in
            std::fs::read_dir(results_dir).wrap_err("read results directory")?
        {
            let timestamp = timestamp.wrap_err("incorrect directory entry")?;
            // read the configuration of this experiment
            let exp_config_path = format!(
                "{}/exp_config.json",
                timestamp.path().as_path().display()
            );
            let exp_config: ExperimentConfig = fantoch_exp::deserialize(
                exp_config_path,
                SerializationFormat::Json,
            )
            .wrap_err("deserialize experiment config")?;

            // incrementally load data as it matched against some search
            let exp_data = None;
            results.push((timestamp, exp_config, exp_data));
        }

        Ok(Self { results })
    }

    pub fn find(
        &mut self,
        search: Search,
    ) -> Result<Vec<&ExperimentData>, Report> {
        // do the search
        let filtered =
            self.results.iter_mut().filter(move |(_, exp_config, _)| {
                // filter out configurations with different n
                if exp_config.config.n() != search.n {
                    return false;
                }

                // filter out configurations with different f
                if exp_config.config.f() != search.f {
                    return false;
                }

                // filter out configurations with different protocol
                if exp_config.protocol != search.protocol {
                    return false;
                }

                // filter out configurations with different clients_per_region
                // (if set)
                if let Some(clients_per_region) = search.clients_per_region {
                    if exp_config.clients_per_region != clients_per_region {
                        return false;
                    }
                }

                // filter out configurations with different key generator (if
                // set)
                if let Some(key_gen) = search.key_gen {
                    if exp_config.workload.key_gen() != key_gen {
                        return false;
                    }
                }

                // filter out configurations with different keys_per_command (if
                // set)
                if let Some(keys_per_command) = search.keys_per_command {
                    if exp_config.workload.keys_per_command()
                        != keys_per_command
                    {
                        return false;
                    }
                }

                // filter out configurations with different payload_size (if
                // set)
                if let Some(payload_size) = search.payload_size {
                    if exp_config.workload.payload_size() != payload_size {
                        return false;
                    }
                }

                // if this exp config was not filtered-out until now, then
                // return it
                true
            });

        let mut results = Vec::new();
        for data in filtered.map(Self::load_experiment_data) {
            let data = data.wrap_err("load experiment data")?;
            results.push(data);
        }
        Ok(results)
    }

    fn load_experiment_data(
        (timestamp, exp_config, exp_data): &mut (
            DirEntry,
            ExperimentConfig,
            Option<ExperimentData>,
        ),
    ) -> Result<&ExperimentData, Report> {
        // load data if `exp_data` is still `None`
        if exp_data.is_none() {
            // process and client metrics
            let mut process_metrics = HashMap::new();
            let mut client_metrics = HashMap::new();

            for region in exp_config.regions.keys() {
                // load metrics
                // let process: ProcessMetrics =
                //     Self::load_metrics("server", &timestamp, region)?;
                // process_metrics.insert(region.clone(), process);

                let client: ClientData =
                    Self::load_metrics("client", &timestamp, region)?;
                client_metrics.insert(region.clone(), client);
            }

            // clean-up client data
            let (start, end) =
                Self::prune_before_last_start_and_after_first_end(
                    &mut client_metrics,
                )?;

            // create global client data (from cleaned-up client data)
            let global_client_metrics =
                Self::global_client_metrics(&client_metrics);

            // process dstats (ignore client dstats, at least for now)
            let mut process_dstats = HashMap::new();

            for region in exp_config.regions.keys() {
                // load dstats
                let process =
                    Self::load_dstat(start, end, "server", &timestamp, region)?;
                process_dstats.insert(region.clone(), process);
            }

            // return experiment data
            *exp_data = Some(ExperimentData::new(
                &exp_config.planet,
                exp_config.testbed,
                process_metrics,
                process_dstats,
                client_metrics,
                global_client_metrics,
            ));
        }

        // at this point `exp_data` must be `Some`
        Ok(exp_data.as_ref().unwrap())
    }

    fn load_metrics<T>(
        tag: &str,
        timestamp: &DirEntry,
        region: &Region,
    ) -> Result<T, Report>
    where
        T: serde::de::DeserializeOwned,
    {
        let path = format!(
            "{}/{}_{}_metrics.bincode",
            timestamp.path().display(),
            tag,
            region.name(),
        );
        let metrics =
            fantoch_exp::deserialize(path, SerializationFormat::Bincode)
                .wrap_err("deserialize metrics data")?;
        Ok(metrics)
    }

    fn load_dstat(
        start: u64,
        end: u64,
        tag: &str,
        timestamp: &DirEntry,
        region: &Region,
    ) -> Result<Dstat, Report> {
        let path = format!(
            "{}/{}_{}_dstat.csv",
            timestamp.path().display(),
            tag,
            region.name(),
        );
        Dstat::new(start, end, path)
    }

    // Here we make sure that we will only consider that points in which all the
    // clients are running, i.e. we prune data points that are from
    // - before the last client starting (i.e. the max of all start times)
    // - after the first client ending (i.e. the min of all end times)
    fn prune_before_last_start_and_after_first_end(
        client_metrics: &mut HashMap<Region, ClientData>,
    ) -> Result<(u64, u64), Report> {
        // compute start and end times for all clients
        let mut starts = Vec::with_capacity(client_metrics.len());
        let mut ends = Vec::with_capacity(client_metrics.len());
        for client_data in client_metrics.values() {
            let bounds = client_data.start_and_end();
            let (start, end) = if let Some(bounds) = bounds {
                bounds
            } else {
                eyre::bail!(
                    "found empty client data without start and end times"
                );
            };
            starts.push(start);
            ends.push(end);
        }

        // compute the global start and end
        let start =
            starts.into_iter().max().expect("global start should exist");
        let end = ends.into_iter().min().expect("global end should exist");

        // prune client data outside of global start and end
        for (_, client_data) in client_metrics.iter_mut() {
            client_data.prune(start, end);
        }

        Ok((start, end))
    }

    // Merge all `ClientData` to get a global view.
    fn global_client_metrics(
        client_metrics: &HashMap<Region, ClientData>,
    ) -> ClientData {
        let mut global = ClientData::new();
        for client_data in client_metrics.values() {
            global.merge(client_data);
        }
        global
    }
}
