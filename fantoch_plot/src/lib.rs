use pyo3::prelude::*;
pub struct Matplotlib<'p> {
    plt: &'p PyModule,
}

impl<'p> Matplotlib<'p> {
    pub fn new(py: Python<'p>) -> PyResult<Self> {
        let plt = PyModule::import(py, "matplotlib.pyplot")?;
        Ok(Self { plt })
    }

    pub fn subplot(
        &self,
        nrows: usize,
        ncols: usize,
        index: usize,
    ) -> PyResult<()> {
        self.plt.call1("subplot", (nrows, ncols, index))?;
        Ok(())
    }

    // TODO maybe take an optional `Fmt` struct instead
    pub fn plot<X, Y>(&self, x: Vec<X>, y: Vec<Y>, fmt: &str) -> PyResult<()>
    where
        X: IntoPy<PyObject>,
        Y: IntoPy<PyObject>,
    {
        self.plt.call1("plot", (x, y, fmt))?;
        Ok(())
    }

    pub fn title(&self, title: &str) -> PyResult<()> {
        self.plt.call1("title", (title,))?;
        Ok(())
    }

    pub fn xlabel(&self, label: &str) -> PyResult<()> {
        self.plt.call1("xlabel", (label,))?;
        Ok(())
    }

    pub fn ylabel(&self, label: &str) -> PyResult<()> {
        self.plt.call1("ylabel", (label,))?;
        Ok(())
    }

    pub fn savefig(&self, path: &str) -> PyResult<()> {
        self.plt.call1("savefig", (path,))?;
        Ok(())
    }
}
