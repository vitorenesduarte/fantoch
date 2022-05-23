pub mod axes;
pub mod axis;
pub mod figure;
pub mod pyplot;
pub mod spines;
pub mod table;

use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyNativeType;

#[macro_export]
macro_rules! pytry {
    ($py:expr, $e:expr) => {{
        match $e {
            Ok(v) => v,
            Err(e) => {
                let e: PyErr = e.into();
                color_eyre::eyre::bail!("{:?}", e.print($py))
            }
        }
    }};
}

#[macro_export]
macro_rules! pydict {
    ($py:expr, $($tup:expr),*) => {{
        #[allow(unused_mut)]
        let mut dict = pyo3::types::PyDict::new($py);
        let mut res = Ok(dict);
        $(
            let (key, value) = $tup;
            if let Err(e) = dict.set_item(key, value) {
                res = Err(e);
            }
        )*
        $crate::pytry!($py, res)
    }};
    ($py:expr, $($tup:expr,)*) => {{
        $crate::pydict![$py, $($tup),*]
    }};
}

pub struct Matplotlib<'p> {
    lib: &'p PyModule,
}

impl<'p> Matplotlib<'p> {
    pub fn new(py: Python<'p>, newsgott: bool) -> Result<Self, Report> {
        // create matplotlib
        let lib = pytry!(py, PyModule::import(py, "matplotlib"));
        let lib = Self { lib };

        // maybe set NewsGotT as font
        if newsgott {
            // load font
            let font_manager =
                pytry!(py, PyModule::import(py, "matplotlib.font_manager"));
            let fonts: Vec<&str> = font_manager
                .call_method1("findSystemFonts", ("./fonts/",))?
                .extract()?;
            for font in fonts {
                font_manager
                    .getattr("fontManager")?
                    .call_method1("addfont", (font,))?;
            }

            // set font
            let kwargs = pydict!(py, ("family", "NewsGotT"));
            lib.rc("font", Some(kwargs))?;
        }

        Ok(lib)
    }

    pub fn rc(
        &self,
        name: &str,
        kwargs: Option<&PyDict>,
    ) -> Result<(), Report> {
        pytry!(self.lib.py(), self.lib.getattr("rc")?.call((name,), kwargs));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyplot::PyPlot;

    // Failing in CI for some reason: https://github.com/vitorenesduarte/fantoch/pull/209
    #[ignore]
    #[test]
    fn save_pdf_test() {
        let path = ".test.pdf";
        if let Err(e) = save_pdf(path) {
            panic!("error while saving pdf: {:?}", e);
        }

        // check that the file was indeed created
        assert_eq!(std::path::Path::new(path).is_file(), true);
    }

    fn save_pdf(path: &str) -> Result<(), Report> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let plt = PyPlot::new(py)?;

        let x = vec!["us-east-1", "ca-central-1", "eu-west-2"];
        let y = vec![10, 20, 30];
        let (fig, ax) = plt.subplots(None)?;
        ax.plot(x, y, Some("o-"), None)?;
        ax.set_xlabel("regions", None)?;
        ax.set_ylabel("latency (ms)", None)?;

        let kwargs = pydict!(py, ("format", "pdf"));
        plt.savefig(path, Some(kwargs))?;

        let kwargs = pydict!(py, ("fig", fig.fig()));
        plt.close(Some(kwargs))?;
        Ok(())
    }
}
