use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Deserialize, Serialize,
)]
pub struct Region {
    name: String,
}

impl Region {
    /// Create a new `Region`.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Region { name: name.into() }
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

impl fmt::Debug for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
