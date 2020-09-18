use crate::pytry;
use color_eyre::Report;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyNativeType;

pub struct Spines<'a> {
    spines: &'a PyDict,
}

impl<'a> Spines<'a> {
    pub fn new(spines: &'a PyDict) -> Self {
        Self { spines }
    }

    // TODO provide instead methods `Spines::values` and `Spine::set_visible`.
    pub fn set_all_visible(&self, visible: bool) -> Result<(), Report> {
        for spine in self.spines.values() {
            pytry!(self.py(), spine.call_method1("set_visible", (visible,)));
        }
        Ok(())
    }

    fn py(&self) -> Python<'_> {
        self.spines.py()
    }
}
