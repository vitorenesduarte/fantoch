use color_eyre::eyre::{self, WrapErr};
use color_eyre::Report;
use fantoch::client::ClientData;
use fantoch::metrics::Histogram;
use fantoch::planet::Region;
use fantoch_exp::{ExperimentConfig, Protocol};
use std::collections::HashMap;
use std::fs::DirEntry;

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
                "{}/exp_config.bincode",
                timestamp.path().as_path().display()
            );
            let exp_config: ExperimentConfig =
                fantoch_exp::deserialize(exp_config_path)
                    .wrap_err("deserialize experiment config")?;

            // incrementally load data as it matched against some search
            let exp_data = None;
            results.push((timestamp, exp_config, exp_data));
        }

        Ok(Self { results })
    }

    pub fn search(&mut self) -> SearchBuilder {
        SearchBuilder::new(self)
    }
}

pub struct SearchBuilder<'a> {
    db: &'a mut ResultsDB,
    n: Option<usize>,
    f: Option<usize>,
    protocol: Option<Protocol>,
    clients_per_region: Option<usize>,
    conflict_rate: Option<usize>,
    payload_size: Option<usize>,
}

impl<'a> SearchBuilder<'a> {
    fn new(db: &'a mut ResultsDB) -> Self {
        Self {
            db,
            n: None,
            f: None,
            protocol: None,
            clients_per_region: None,
            conflict_rate: None,
            payload_size: None,
        }
    }

    pub fn n(&mut self, n: usize) -> &mut Self {
        self.n = Some(n);
        self
    }

    pub fn f(&mut self, f: usize) -> &mut Self {
        self.f = Some(f);
        self
    }

    pub fn protocol(&mut self, protocol: Protocol) -> &mut Self {
        self.protocol = Some(protocol);
        self
    }

    pub fn clients_per_region(
        &mut self,
        clients_per_region: usize,
    ) -> &mut Self {
        self.clients_per_region = Some(clients_per_region);
        self
    }

    pub fn conflict_rate(&mut self, conflict_rate: usize) -> &mut Self {
        self.conflict_rate = Some(conflict_rate);
        self
    }

    pub fn payload_size(&mut self, payload_size: usize) -> &mut Self {
        self.payload_size = Some(payload_size);
        self
    }

    pub fn load(&mut self) -> Result<Vec<ExperimentData>, Report> {
        let mut results = Vec::new();
        for data in self.find().map(Self::load_experiment_data) {
            let data = data.wrap_err("load experiment data")?;
            results.push(data);
        }
        Ok(results)
    }

    fn find(
        &mut self,
    ) -> impl Iterator<
        Item = &mut (DirEntry, ExperimentConfig, Option<ExperimentData>),
    > {
        // let's make the borrow checker happy
        let n = self.n;
        let f = self.f;
        let protocol = self.protocol;
        let clients_per_region = self.clients_per_region;
        let conflict_rate = self.conflict_rate;
        let payload_size = self.payload_size;

        // do the search
        self.db
            .results
            .iter_mut()
            .filter(move |(_, exp_config, _)| {
                // filter out configurations with different n (if set)
                if let Some(n) = n {
                    if exp_config.config.n() != n {
                        return false;
                    }
                }

                // filter out configurations with different f (if set)
                if let Some(f) = f {
                    if exp_config.config.f() != f {
                        return false;
                    }
                }

                // filter out configurations with different protocol (if set)
                if let Some(protocol) = protocol {
                    if exp_config.protocol != protocol {
                        return false;
                    }
                }

                // filter out configurations with different clients_per_region
                // (if set)
                if let Some(clients_per_region) = clients_per_region {
                    if exp_config.clients_per_region != clients_per_region {
                        return false;
                    }
                }

                // filter out configurations with different conflict_rate (if
                // set)
                if let Some(conflict_rate) = conflict_rate {
                    if exp_config.conflict_rate != conflict_rate {
                        return false;
                    }
                }

                // filter out configurations with different payload_size (if
                // set)
                if let Some(payload_size) = payload_size {
                    if exp_config.payload_size != payload_size {
                        return false;
                    }
                }

                // if this exp config was not filtered-out until now, then
                // return it
                true
            })
    }

    fn load_experiment_data(
        (timestamp, exp_config, exp_data): &mut (
            DirEntry,
            ExperimentConfig,
            Option<ExperimentData>,
        ),
    ) -> Result<ExperimentData, Report> {
        // load data if `exp_data` is still `None`
        if exp_data.is_none() {
            let mut client_metrics = HashMap::new();

            for region in exp_config.regions.keys() {
                let path = format!(
                    "{}/client_{}_metrics.bincode",
                    timestamp.path().display(),
                    region.name()
                );
                let client_data: ClientData = fantoch_exp::deserialize(path)
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
                client_metrics,
                global_client_metrics,
            ));
        }

        // at this point `exp_data` must be `Some`
        Ok(exp_data.clone().unwrap())
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

#[derive(Debug, Clone)]
pub struct ExperimentData {
    pub client_metrics: HashMap<Region, ClientData>,
    pub global_client_metrics: ClientData,
    pub client_latency: HashMap<Region, Histogram>,
    pub global_client_latency: Histogram,
}

impl ExperimentData {
    fn new(
        client_metrics: HashMap<Region, ClientData>,
        global_client_metrics: ClientData,
    ) -> Self {
        let client_latency = client_metrics
            .clone()
            .into_iter()
            .map(|(region, client_data)| {
                // create latency histogram
                let histogram = Histogram::from(client_data.latency_data());
                (region, histogram)
            })
            .collect();
        let global_client_latency =
            Histogram::from(global_client_metrics.latency_data());
        Self {
            client_metrics,
            global_client_metrics,
            client_latency,
            global_client_latency,
        }
    }
}
