use pyo3::prelude::*;
use pyo3::types::PyDict;

pub struct Figure<'a> {
    fig: &'a PyAny,
}

impl<'a> Figure<'a> {
    pub fn new(fig: &'a PyAny) -> Self {
        Self { fig }
    }

    pub fn fig(&self) -> &PyAny {
        self.fig
    }

    pub fn subplots_adjust(&self, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.fig.call_method("subplots_adjust", (), kwargs)?;
        Ok(())
    }
}
