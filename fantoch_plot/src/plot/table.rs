use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::PyNativeType;

// https://matplotlib.org/api/table_api.html#matplotlib.table.Table
pub struct Table<'a> {
    table: &'a PyAny,
}

impl<'a> Table<'a> {
    pub fn new(table: &'a PyAny) -> Self {
        Self { table }
    }

    pub fn auto_set_font_size(&self, auto: bool) -> Result<(), Report> {
        pytry!(
            self.py(),
            self.table.call_method1("auto_set_font_size", (auto,))
        );
        Ok(())
    }

    pub fn set_fontsize(&self, size: f64) -> Result<(), Report> {
        pytry!(self.py(), self.table.call_method1("set_fontsize", (size,)));
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.table.py()
    }
}
