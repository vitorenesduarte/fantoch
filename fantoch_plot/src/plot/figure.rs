use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyNativeType;

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

    pub fn subplots_adjust(
        &self,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.fig.call_method("subplots_adjust", (), kwargs)
        );
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.fig.py()
    }
}
