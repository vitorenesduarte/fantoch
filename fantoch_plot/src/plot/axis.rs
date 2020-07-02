use pyo3::prelude::*;

// https://matplotlib.org/api/axis_api.html?highlight=axis#matplotlib.axis.Axis
pub struct Axis<'a> {
    axis: &'a PyAny,
}

impl<'a> Axis<'a> {
    pub fn new(axis: &'a PyAny) -> Self {
        Self { axis }
    }

    pub fn set_major_formatter(&self, formatter: &PyAny) -> PyResult<()> {
        self.axis
            .call_method1("set_major_formatter", (formatter,))?;
        Ok(())
    }

    pub fn set_minor_formatter(&self, formatter: &PyAny) -> PyResult<()> {
        self.axis
            .call_method1("set_minor_formatter", (formatter,))?;
        Ok(())
    }

    pub fn set_visible(&self, visible: bool) -> PyResult<()> {
        self.axis.call_method1("set_visible", (visible,))?;
        Ok(())
    }
}
