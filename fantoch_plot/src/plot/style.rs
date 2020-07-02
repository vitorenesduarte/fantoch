use pyo3::prelude::*;

pub struct Style<'p> {
    style: &'p PyModule,
}

impl<'p> Style<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let style = PyModule::import(py, "matplotlib.style")?;
        Ok(Self { style })
    }

    pub fn use_(&self, style: &str) -> PyResult<()> {
        self.style.call1("use", (style,))?;
        Ok(())
    }
}
