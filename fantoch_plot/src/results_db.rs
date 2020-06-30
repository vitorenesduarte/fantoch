use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use fantoch_exp::{ExperimentConfig, Protocol};
use std::fs::DirEntry;

#[derive(Debug)]
pub struct ResultsDB {
    results: Vec<(DirEntry, ExperimentConfig)>,
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

            results.push((timestamp, exp_config));
        }

        Ok(Self { results })
    }

    pub fn search(&self) -> SearchBuilder {
        SearchBuilder::new(&self)
    }
}

pub struct SearchBuilder<'a> {
    db: &'a ResultsDB,
    n: Option<usize>,
    f: Option<usize>,
    protocol: Option<Protocol>,
    clients_per_region: Option<usize>,
}

impl<'a> SearchBuilder<'a> {
    fn new(db: &'a ResultsDB) -> Self {
        Self {
            db,
            n: None,
            f: None,
            protocol: None,
            clients_per_region: None,
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

    pub fn find(&self) -> impl Iterator<Item = &ExperimentConfig> {
        self.db
            .results
            .iter()
            .filter(move |(_, exp_config)| {
                // filter out configurations with different n (if set)
                if let Some(n) = self.n {
                    if exp_config.config.n() != n {
                        return false;
                    }
                }

                // filter out configurations with different f (if set)
                if let Some(f) = self.f {
                    if exp_config.config.f() != f {
                        return false;
                    }
                }

                // filter out configurations with different protocol (if set)
                if let Some(protocol) = self.protocol {
                    if exp_config.protocol != protocol {
                        return false;
                    }
                }

                // filter out configurations with different clients_per_region
                // (if set)
                if let Some(clients_per_region) = self.clients_per_region {
                    if exp_config.clients_per_region != clients_per_region {
                        return false;
                    }
                }

                // if this exp config was not filtered-out until now, then
                // return it
                true
            })
            .map(|(_, exp_config)| exp_config)
    }
}
