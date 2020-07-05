use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::PyNativeType;

// https://matplotlib.org/api/axis_api.html?highlight=axis#matplotlib.axis.Axis
pub struct Axis<'a> {
    axis: &'a PyAny,
}

impl<'a> Axis<'a> {
    pub fn new(axis: &'a PyAny) -> Self {
        Self { axis }
    }

    pub fn set_visible(&self, visible: bool) -> Result<(), Report> {
        pytry!(self.py(), self.axis.call_method1("set_visible", (visible,)));
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.axis.py()
    }
}
