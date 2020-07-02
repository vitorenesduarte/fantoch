use pyo3::prelude::*;

pub struct Ticker<'p> {
    ticker: &'p PyModule,
}

impl<'p> Ticker<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let ticker = PyModule::import(py, "matplotlib.ticker")?;
        Ok(Self { ticker })
    }

    // Returns a `ScalarFormatter`.
    pub fn scalar_formatter(&self) -> PyResult<&PyAny> {
        self.ticker.call0("ScalarFormatter")
    }
}
