use crate::db::dstat::Dstat;
use crate::db::exp_data::ExperimentData;
use crate::Search;
use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch::client::ClientData;
use fantoch::planet::Region;
use fantoch::run::task::metrics_logger::ProcessMetrics;
use fantoch_exp::{ExperimentConfig, SerializationFormat};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::DirEntry;

#[derive(Debug)]
pub struct ResultsDB {
    results: Vec<(DirEntry, ExperimentConfig, ExperimentData)>,
}

impl ResultsDB {
    pub fn load(results_dir: &str) -> Result<Self, Report> {
        // find all timestamps
        let timestamps: Vec<_> = std::fs::read_dir(results_dir)
            .wrap_err("read results directory")?
            .collect();

        // holder for results
        let mut results = Vec::with_capacity(timestamps.len());

        // load all entries
        let loads: Vec<_> = timestamps
            .into_par_iter()
            .map(|timestamp| Self::load_entry(timestamp))
            .collect();
        for entry in loads {
            let entry = entry.wrap_err("load entry")?;
            results.push(entry);
        }

        Ok(Self { results })
    }

    fn load_entry(
        timestamp: Result<DirEntry, std::io::Error>,
    ) -> Result<(DirEntry, ExperimentConfig, ExperimentData), Report> {
        let timestamp = timestamp.wrap_err("incorrect directory entry")?;
        // read the configuration of this experiment
        let exp_config_path =
            format!("{}/exp_config.json", timestamp.path().as_path().display());
        let exp_config: ExperimentConfig = fantoch_exp::deserialize(
            exp_config_path,
            SerializationFormat::Json,
        )
        .wrap_err("deserialize experiment config")?;

        let exp_data = Self::load_experiment_data(&timestamp, &exp_config)?;
        Ok((timestamp, exp_config, exp_data))
    }

    pub fn find(
        &mut self,
        search: Search,
    ) -> Result<Vec<&ExperimentData>, Report> {
        let filtered = self
            .results
            .iter()
            .filter(move |(_, exp_config, _)| {
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
                    if exp_config.workload.keys_per_shard() != keys_per_command
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
            })
            .map(|(_, _, exp_data)| exp_data)
            .collect();
        Ok(filtered)
    }

    fn load_experiment_data(
        timestamp: &DirEntry,
        exp_config: &ExperimentConfig,
    ) -> Result<ExperimentData, Report> {
        // client metrics
        let mut client_metrics = HashMap::new();

        for (region, _) in exp_config.placement.keys() {
            // create client file prefix
            let prefix = fantoch_exp::config::file_prefix(None, region);

            // load this region's client metrics (there's a single client
            // machine per region)
            let client: ClientData = Self::load_metrics(&timestamp, prefix)?;
            client_metrics.insert(region.clone(), client);
        }

        // clean-up client data
        let (start, end) = Self::prune_before_last_start_and_after_first_end(
            &mut client_metrics,
        )?;

        // create global client data (from cleaned-up client data)
        let global_client_metrics =
            Self::global_client_metrics(&client_metrics);

        // process metrics and dstats
        let mut process_metrics = HashMap::new();
        let mut process_dstats = HashMap::new();

        for ((region, _), (process_id, _)) in exp_config.placement.iter() {
            let process_id = *process_id;
            // create process file prefix
            let prefix =
                fantoch_exp::config::file_prefix(Some(process_id), region);

            // load this process metrics (there will be more than one per region
            // with partial replication)
            let process: ProcessMetrics =
                Self::load_metrics(&timestamp, prefix.clone())?;
            process_metrics.insert(process_id, (region.clone(), process));

            // load this process dstat
            let process = Self::load_dstat(&timestamp, prefix, start, end)?;
            process_dstats.insert(process_id, process);
        }
        // return experiment data
        Ok(ExperimentData::new(
            &exp_config.planet,
            exp_config.testbed,
            process_metrics,
            process_dstats,
            client_metrics,
            global_client_metrics,
        ))
    }

    fn load_metrics<T>(
        timestamp: &DirEntry,
        prefix: String,
    ) -> Result<T, Report>
    where
        T: serde::de::DeserializeOwned,
    {
        let path = format!(
            "{}/{}_metrics.bincode",
            timestamp.path().display(),
            prefix,
        );
        let metrics =
            fantoch_exp::deserialize(&path, SerializationFormat::Bincode)
                .wrap_err_with(|| format!("deserialize metrics {}", path))?;
        Ok(metrics)
    }

    fn load_dstat(
        timestamp: &DirEntry,
        prefix: String,
        start: u64,
        end: u64,
    ) -> Result<Dstat, Report> {
        let path =
            format!("{}/{}_dstat.csv", timestamp.path().display(), prefix);
        Dstat::from(start, end, &path)
            .wrap_err_with(|| format!("deserialize dstat {}", path))
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
