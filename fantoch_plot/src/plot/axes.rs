use crate::plot::axis::Axis;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub struct Axes<'a> {
    ax: &'a PyAny,
    pub xaxis: Axis<'a>,
    pub yaxis: Axis<'a>,
}

impl<'a> Axes<'a> {
    pub fn new(ax: &'a PyAny) -> PyResult<Self> {
        let xaxis = Axis::new(ax.getattr("xaxis")?);
        let yaxis = Axis::new(ax.getattr("yaxis")?);
        Ok(Self { ax, xaxis, yaxis })
    }

    pub fn set_title(&self, title: &str) -> PyResult<()> {
        self.ax.call_method1("set_title", (title,))?;
        Ok(())
    }

    pub fn set_xlabel(&self, label: &str) -> PyResult<()> {
        self.ax.call_method1("set_xlabel", (label,))?;
        Ok(())
    }

    pub fn set_ylabel(&self, label: &str) -> PyResult<()> {
        self.ax.call_method1("set_ylabel", (label,))?;
        Ok(())
    }

    pub fn set_xticks<T>(&self, ticks: Vec<T>) -> PyResult<()>
    where
        T: IntoPy<PyObject>,
    {
        self.ax.call_method1("set_xticks", (ticks,))?;
        Ok(())
    }

    pub fn set_xticklabels<L>(&self, labels: Vec<L>) -> PyResult<()>
    where
        L: IntoPy<PyObject>,
    {
        self.ax.call_method1("set_xticklabels", (labels,))?;
        Ok(())
    }

    pub fn set_xscale(&self, value: &str) -> PyResult<()> {
        self.ax.call_method1("set_xscale", (value,))?;
        Ok(())
    }

    pub fn set_yscale(&self, value: &str) -> PyResult<()> {
        self.ax.call_method1("set_yscale", (value,))?;
        Ok(())
    }

    pub fn ticklabel_format(&self, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.ax.call_method("ticklabel_format", (), kwargs)?;
        Ok(())
    }

    pub fn set_xlim(&self, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.ax.call_method("set_xlim", (), kwargs)?;
        Ok(())
    }

    pub fn set_ylim(&self, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.ax.call_method("set_ylim", (), kwargs)?;
        Ok(())
    }

    // any questions about legend positioning should be answered here: https://stackoverflow.com/a/43439132/4262469
    // - that's how great the answer is!
    pub fn legend(&self, kwargs: Option<&PyDict>) -> PyResult<()> {
        self.ax.call_method("legend", (), kwargs)?;
        Ok(())
    }

    pub fn plot<X, Y>(
        &self,
        x: Vec<X>,
        y: Vec<Y>,
        fmt: Option<&str>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<()>
    where
        X: IntoPy<PyObject>,
        Y: IntoPy<PyObject>,
    {
        if let Some(fmt) = fmt {
            self.ax.call_method("plot", (x, y, fmt), kwargs)?;
        } else {
            self.ax.call_method("plot", (x, y), kwargs)?;
        };
        Ok(())
    }

    pub fn bar<X, H>(
        &self,
        x: Vec<X>,
        height: Vec<H>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<()>
    where
        X: IntoPy<PyObject>,
        H: IntoPy<PyObject>,
    {
        self.ax.call_method("bar", (x, height), kwargs)?;
        Ok(())
    }
}
