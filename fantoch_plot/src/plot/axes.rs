use crate::plot::axis::Axis;
use crate::plot::spines::Spines;
use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyTuple};
use pyo3::PyNativeType;

pub struct Axes<'a> {
    ax: &'a PyAny,
    pub xaxis: Axis<'a>,
    pub yaxis: Axis<'a>,
    pub spines: Spines<'a>,
}

impl<'a> Axes<'a> {
    pub fn new(ax: &'a PyAny) -> Result<Self, Report> {
        let xaxis = Axis::new(pytry!(ax.py(), ax.getattr("xaxis")));
        let yaxis = Axis::new(pytry!(ax.py(), ax.getattr("yaxis")));
        let spines = pytry!(ax.py(), ax.getattr("spines"));
        let spines = pytry!(ax.py(), spines.downcast::<PyDict>());
        let spines = Spines::new(spines);
        Ok(Self {
            ax,
            xaxis,
            yaxis,
            spines,
        })
    }

    pub fn ax(&self) -> &PyAny {
        self.ax
    }

    pub fn grid(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method("grid", (), kwargs));
        Ok(())
    }

    pub fn set_xlabel(&self, label: &str) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method1("set_xlabel", (label,)));
        Ok(())
    }

    pub fn set_ylabel(&self, label: &str) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method1("set_ylabel", (label,)));
        Ok(())
    }

    pub fn set_xticks<T>(
        &self,
        ticks: Vec<T>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        T: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.ax.call_method("set_xticks", (ticks,), kwargs)
        );
        Ok(())
    }

    pub fn set_yticks<T>(
        &self,
        ticks: Vec<T>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        T: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.ax.call_method("set_yticks", (ticks,), kwargs)
        );
        Ok(())
    }

    pub fn set_xticklabels<L>(
        &self,
        labels: Vec<L>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        L: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.ax.call_method("set_xticklabels", (labels,), kwargs)
        );
        Ok(())
    }

    pub fn set_yticklabels<L>(
        &self,
        labels: Vec<L>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        L: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.ax.call_method("set_yticklabels", (labels,), kwargs)
        );
        Ok(())
    }

    pub fn tick_params(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method("tick_params", (), kwargs));
        Ok(())
    }

    pub fn set_xscale(&self, value: &str) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method1("set_xscale", (value,)));
        Ok(())
    }

    pub fn set_yscale(&self, value: &str) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method1("set_yscale", (value,)));
        Ok(())
    }

    pub fn get_xlim(&self) -> Result<(f64, f64), Report> {
        let xlim = pytry!(self.py(), self.ax.call_method0("get_xlim"));
        let xlim = pytry!(self.py(), xlim.downcast::<PyTuple>());
        let left = pytry!(self.py(), xlim.get_item(0).downcast::<PyFloat>());
        let right = pytry!(self.py(), xlim.get_item(1).downcast::<PyFloat>());
        Ok((left.value(), right.value()))
    }

    pub fn set_xlim(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method("set_xlim", (), kwargs));
        Ok(())
    }

    pub fn get_ylim(&self) -> Result<(f64, f64), Report> {
        let xlim = pytry!(self.py(), self.ax.call_method0("get_ylim"));
        let xlim = pytry!(self.py(), xlim.downcast::<PyTuple>());
        let left = pytry!(self.py(), xlim.get_item(0).downcast::<PyFloat>());
        let right = pytry!(self.py(), xlim.get_item(1).downcast::<PyFloat>());
        Ok((left.value(), right.value()))
    }

    pub fn set_ylim(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method("set_ylim", (), kwargs));
        Ok(())
    }

    // any questions about legend positioning should be answered here: https://stackoverflow.com/a/43439132/4262469
    // - that's how great the answer is!
    pub fn legend(&self, kwargs: Option<&PyDict>) -> Result<(), Report> {
        pytry!(self.py(), self.ax.call_method("legend", (), kwargs));
        Ok(())
    }

    pub fn plot<X, Y>(
        &self,
        x: Vec<X>,
        y: Vec<Y>,
        fmt: Option<&str>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        X: IntoPy<PyObject>,
        Y: IntoPy<PyObject>,
    {
        if let Some(fmt) = fmt {
            pytry!(self.py(), self.ax.call_method("plot", (x, y, fmt), kwargs));
        } else {
            pytry!(self.py(), self.ax.call_method("plot", (x, y), kwargs));
        };
        Ok(())
    }

    pub fn bar<X, H>(
        &self,
        x: Vec<X>,
        height: Vec<H>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        X: IntoPy<PyObject>,
        H: IntoPy<PyObject>,
    {
        pytry!(self.py(), self.ax.call_method("bar", (x, height), kwargs));
        Ok(())
    }

    pub fn imshow<D>(
        &self,
        data: Vec<D>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        D: IntoPy<PyObject>,
    {
        pytry!(self.py(), self.ax.call_method("imshow", (data,), kwargs));
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.ax.py()
    }
}
