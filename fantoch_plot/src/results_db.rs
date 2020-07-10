use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch::client::{ClientData, KeyGen};
use fantoch::metrics::Histogram;
use fantoch::planet::{Planet, Region};
use fantoch_exp::{ExperimentConfig, Protocol, SerializationFormat, Testbed};
use std::collections::HashMap;
use std::fs::DirEntry;
use std::time::Duration;

#[derive(Debug)]
pub struct ResultsDB {
    results: Vec<(DirEntry, ExperimentConfig, Option<ExperimentData>)>,
}

impl ResultsDB {
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
            let mut client_metrics = HashMap::new();

            for region in exp_config.regions.keys() {
                let path = format!(
                    "{}/client_{}_metrics.bincode",
                    timestamp.path().display(),
                    region.name(),
                );
                let client_data: ClientData = fantoch_exp::deserialize(
                    path,
                    SerializationFormat::Bincode,
                )
                .wrap_err("deserialize client data")?;
                let res = client_metrics.insert(region.clone(), client_data);
                assert!(res.is_none());
            }

            // clean-up client data
            Self::prune_before_last_start_and_after_first_end(
                &mut client_metrics,
            )?;

            // create global client data
            let global_client_metrics =
                Self::global_client_metrics(&client_metrics);

            // return experiment data
            *exp_data = Some(ExperimentData::new(
                &exp_config.planet,
                exp_config.testbed,
                client_metrics,
                global_client_metrics,
            ));
        }

        // at this point `exp_data` must be `Some`
        Ok(exp_data.as_ref().unwrap())
    }

    // Here we make sure that we will only consider that points in which all the
    // clients are running, i.e. we prune data points that are from
    // - before the last client starting (i.e. the max of all start times)
    // - after the first client ending (i.e. the min of all end times)
    fn prune_before_last_start_and_after_first_end(
        client_metrics: &mut HashMap<Region, ClientData>,
    ) -> Result<(), Report> {
        let mut starts = Vec::with_capacity(client_metrics.len());
        let mut ends = Vec::with_capacity(client_metrics.len());
        for client_data in client_metrics.values() {
            let (start, end) = if let Some(bounds) = client_data.start_and_end()
            {
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

        for (_, client_data) in client_metrics.iter_mut() {
            client_data.prune(start, end);
        }
        Ok(())
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
#[derive(Clone, Copy)]
pub struct Search {
    pub n: usize,
    pub f: usize,
    pub protocol: Protocol,
    clients_per_region: Option<usize>,
    key_gen: Option<KeyGen>,
    keys_per_command: Option<usize>,
    payload_size: Option<usize>,
}

impl Search {
    pub fn new(n: usize, f: usize, protocol: Protocol) -> Self {
        Self {
            n,
            f,
            protocol,
            clients_per_region: None,
            key_gen: None,
            keys_per_command: None,
            payload_size: None,
        }
    }

    pub fn clients_per_region(
        &mut self,
        clients_per_region: usize,
    ) -> &mut Self {
        self.clients_per_region = Some(clients_per_region);
        self
    }

    pub fn key_gen(&mut self, key_gen: KeyGen) -> &mut Self {
        self.key_gen = Some(key_gen);
        self
    }

    pub fn keys_per_command(&mut self, keys_per_command: usize) -> &mut Self {
        self.keys_per_command = Some(keys_per_command);
        self
    }

    pub fn payload_size(&mut self, payload_size: usize) -> &mut Self {
        self.payload_size = Some(payload_size);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ExperimentData {
    pub client_latency: HashMap<Region, Histogram>,
    pub global_client_latency: Histogram,
}

impl ExperimentData {
    fn new(
        planet: &Option<Planet>,
        testbed: Testbed,
        client_metrics: HashMap<Region, ClientData>,
        global_client_metrics: ClientData,
    ) -> Self {
        // we should use milliseconds if: AWS or baremetal + injected latency
        let precision = match testbed {
            Testbed::Aws => {
                // assert that no latency was injected
                assert!(planet.is_none());
                LatencyPrecision::Millis
            }
            Testbed::Baremetal => {
                // use ms if latency was injected, otherwise micros
                if planet.is_some() {
                    LatencyPrecision::Millis
                } else {
                    LatencyPrecision::Micros
                }
            }
        };

        // create latency histogram per region
        let client_latency = client_metrics
            .into_iter()
            .map(|(region, client_data)| {
                // create latency histogram
                let latency = Self::extract_latency(
                    precision,
                    client_data.latency_data(),
                );
                let histogram = Histogram::from(latency);
                (region, histogram)
            })
            .collect();
        let latency = Self::extract_latency(
            precision,
            global_client_metrics.latency_data(),
        );

        // create global latency histogram
        let global_client_latency = Histogram::from(latency);

        Self {
            client_latency,
            global_client_latency,
        }
    }

    fn extract_latency(
        precision: LatencyPrecision,
        it: impl Iterator<Item = Duration>,
    ) -> impl Iterator<Item = u64> {
        it.map(move |duration| {
            let latency = match precision {
                LatencyPrecision::Micros => duration.as_micros(),
                LatencyPrecision::Millis => duration.as_millis(),
            };
            latency as u64
        })
    }
}

#[derive(Clone, Copy)]
enum LatencyPrecision {
    Micros,
    Millis,
}
