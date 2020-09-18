use crate::plot::axes::Axes;
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

    pub fn set_size_inches(
        &self,
        width: f64,
        height: f64,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.fig.call_method1("set_size_inches", (width, height))
        );
        Ok(())
    }

    pub fn add_axes(&self, dimensions: Vec<f64>) -> Result<Axes<'_>, Report> {
        let ax = Axes::new(pytry!(
            self.py(),
            self.fig.call_method1("add_axes", (dimensions,),)
        ))?;
        Ok(ax)
    }

    pub fn colorbar(
        &self,
        im: &PyAny,
        kwargs: Option<&PyDict>,
    ) -> Result<ColorBar<'_>, Report> {
        let cbar = ColorBar::new(pytry!(
            self.py(),
            self.fig.call_method("colorbar", (im,), kwargs)
        ))?;
        Ok(cbar)
    }

    fn py(&self) -> Python<'_> {
        self.fig.py()
    }
}
pub struct ColorBar<'a> {
    bar: &'a PyAny,
}

impl<'a> ColorBar<'a> {
    pub fn new(bar: &'a PyAny) -> Result<Self, Report> {
        Ok(Self { bar })
    }

    pub fn set_label(
        &self,
        label: &str,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.bar.call_method("set_label", (label,), kwargs)
        );
        Ok(())
    }

    pub fn set_ticks<T>(
        &self,
        ticks: Vec<T>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        T: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.bar.call_method("set_ticks", (ticks,), kwargs)
        );
        Ok(())
    }

    pub fn set_ticklabels<L>(
        &self,
        labels: Vec<L>,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report>
    where
        L: IntoPy<PyObject>,
    {
        pytry!(
            self.py(),
            self.bar.call_method("set_ticklabels", (labels,), kwargs)
        );
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.bar.py()
    }
}
