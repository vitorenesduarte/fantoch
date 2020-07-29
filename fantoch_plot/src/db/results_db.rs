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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::DirEntry;
use std::path::Path;
use std::sync::{Arc, Mutex};

const SNAPSHOT_SUFFIX: &str = "_experiment_data_snapshot.bincode.gz";

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultsDB {
    results: Vec<(ExperimentConfig, ExperimentData)>,
}

impl ResultsDB {
    pub fn load(results_dir: &str) -> Result<Self, Report> {
        // find all timestamps
        let read_dir = std::fs::read_dir(results_dir)
            .wrap_err("read results directory")?;
        let mut timestamps = Vec::new();
        for timestamp in read_dir {
            let timestamp = timestamp.wrap_err("incorrect directory entry")?;
            // ignore snapshot files
            if !timestamp
                .path()
                .display()
                .to_string()
                .ends_with(SNAPSHOT_SUFFIX)
            {
                timestamps.push(timestamp);
            }
        }

        // holder for results
        let mut results = Vec::with_capacity(timestamps.len());

        // track the number of loaded entries
        let loaded_entries = Arc::new(Mutex::new(0));
        let total_entries = timestamps.len();

        // load all entries
        let loads: Vec<_> = timestamps
            .into_par_iter()
            .map(|timestamp| {
                let loaded_entries = loaded_entries.clone();
                Self::load_entry(timestamp, loaded_entries, total_entries)
            })
            .inspect(|entry| {
                if let Err(e) = entry {
                    println!("error: {:?}", e);
                }
            })
            .collect();
        for entry in loads {
            let entry = entry.wrap_err("load entry")?;
            results.push(entry);
        }

        Ok(Self { results })
    }

    fn load_entry(
        timestamp: DirEntry,
        loaded_entries: Arc<Mutex<usize>>,
        total_entries: usize,
    ) -> Result<(ExperimentConfig, ExperimentData), Report> {
        // register load start time
        let start = std::time::Instant::now();

        // read the configuration of this experiment
        let exp_config_path =
            format!("{}/exp_config.json", timestamp.path().display());
        let exp_config: ExperimentConfig = fantoch_exp::deserialize(
            exp_config_path,
            SerializationFormat::Json,
        )
        .wrap_err("deserialize experiment config")?;

        // check if there's snapshot of experiment data
        let snapshot =
            format!("{}{}", timestamp.path().display(), SNAPSHOT_SUFFIX);
        let exp_data = if Path::new(&snapshot).exists() {
            // if there is, simply load it
            fantoch_exp::deserialize(&snapshot, SerializationFormat::BincodeGz)
                .wrap_err_with(|| {
                    format!("deserialize experiment data snapshot {}", snapshot)
                })?
        } else {
            // otherwise load it
            let exp_data = Self::load_experiment_data(&timestamp, &exp_config)?;
            // create snapshot
            fantoch_exp::serialize(
                &exp_data,
                &snapshot,
                SerializationFormat::BincodeGz,
            )
            .wrap_err_with(|| {
                format!("deserialize experiment data snapshot {}", snapshot)
            })?;
            // and return it
            exp_data
        };

        // register that a new entry is loaded
        let mut loaded_entries = loaded_entries
            .lock()
            .expect("locking loaded entries should work");
        *loaded_entries += 1;
        println!(
            "loaded {:?} after {:?} | {} of {}",
            timestamp.path().display(),
            start.elapsed(),
            loaded_entries,
            total_entries,
        );
        Ok((exp_config, exp_data))
    }

    pub fn find(&self, search: Search) -> Result<Vec<&ExperimentData>, Report> {
        let filtered = self
            .results
            .iter()
            .filter(move |(exp_config, _)| {
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

                // filter out configurations with different shards_per_comman
                // d(if set)
                if let Some(shards_per_command) = search.shards_per_command {
                    if exp_config.workload.shards_per_command()
                        != shards_per_command
                    {
                        return false;
                    }
                }

                // filter out configurations with different shard generator (if
                // set)
                if let Some(shard_gen) = search.shard_gen {
                    if exp_config.workload.shard_gen() != shard_gen {
                        return false;
                    }
                }

                // filter out configurations with different keys_per_shard (if
                // set)
                if let Some(keys_per_shard) = search.keys_per_shard {
                    if exp_config.workload.keys_per_shard() != keys_per_shard {
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
            .map(|(_, exp_data)| exp_data)
            .collect();
        Ok(filtered)
    }

    fn load_experiment_data(
        timestamp: &DirEntry,
        exp_config: &ExperimentConfig,
    ) -> Result<ExperimentData, Report> {
        // client metrics
        let mut client_metrics = HashMap::new();

        for (region, _, _, _) in exp_config.placement.iter() {
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

        // client dstats (need to be after processing client metrics so that we
        // have a `start` and an `end` for pruning)
        let mut client_dstats = HashMap::new();

        for (region, _, _, _) in exp_config.placement.iter() {
            // create client file prefix
            let prefix = fantoch_exp::config::file_prefix(None, region);

            // load this region's client dstat
            let client = Self::load_dstat(&timestamp, prefix, start, end)?;
            client_dstats.insert(region.clone(), client);
        }

        // process metrics and dstats
        let mut process_metrics = HashMap::new();
        let mut process_dstats = HashMap::new();

        for (region, _, process_id, _) in exp_config.placement.iter() {
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
            client_dstats,
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
            "{}/{}_metrics.bincode.gz",
            timestamp.path().display(),
            prefix,
        );
        let metrics =
            fantoch_exp::deserialize(&path, SerializationFormat::BincodeGz)
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
