use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

// custom implementation which allows `Region`'s to be used as keys in maps when
// serializing with `serde_json`
impl Serialize for Region {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(d)?;
        Ok(Region::new(name))
    }
}
