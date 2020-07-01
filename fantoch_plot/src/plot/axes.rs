use pyo3::prelude::*;
use pyo3::types::PyDict;

pub enum AxisFormatter {
    Null,
    Scalar,
    FormatStr(&'static str),
}

impl AxisFormatter {
    fn name(&self) -> &str {
        match self {
            Self::Null => "NullFormatter",
            Self::Scalar => "ScalarFormatter",
            Self::FormatStr(_) => "FormatStrFormatter",
        }
    }

    fn constructor(&self) -> String {
        match self {
            Self::Null => String::from("NullFormatter()"),
            Self::Scalar => String::from("ScalarFormatter()"),
            Self::FormatStr(s) => format!("FormatStrFormatter('{}')", s),
        }
    }
}

pub struct Axes<'a> {
    ax: &'a PyAny,
}

impl<'a> Axes<'a> {
    pub fn new(ax: &'a PyAny) -> Self {
        Self { ax }
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

    pub fn axis_set_minor_formatter(
        &self,
        py: Python<'_>,
        axis: &str,
        formatter: AxisFormatter,
    ) -> PyResult<()> {
        self.axis_set_formatter_as_scalar(
            py,
            axis,
            "set_minor_formatter",
            formatter,
        )
    }

    pub fn axis_set_major_formatter(
        &self,
        py: Python<'_>,
        axis: &str,
        formatter: AxisFormatter,
    ) -> PyResult<()> {
        self.axis_set_formatter_as_scalar(
            py,
            axis,
            "set_major_formatter",
            formatter,
        )
    }

    // HACK:
    fn axis_set_formatter_as_scalar(
        &self,
        py: Python<'_>,
        axis: &str,
        method: &str,
        formatter: AxisFormatter,
    ) -> PyResult<()> {
        // create a scalar formatter
        let locals = PyDict::new(py);
        let program = format!(
            "from matplotlib.ticker import {}\nformatter = {}",
            formatter.name(),
            formatter.constructor()
        );
        py.run(&program, None, Some(locals))?;
        let formatter = locals.get_item("formatter").unwrap();

        // get the axis and set the created formatter as the formatter (major or
        // minor, depending on `method`)
        let axis = self.ax.getattr(axis)?;
        axis.call_method1(method, (formatter,))?;
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
