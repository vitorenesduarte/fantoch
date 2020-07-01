use pyo3::prelude::*;

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
}
