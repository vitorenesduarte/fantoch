use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialOrd, PartialEq, Deserialize, Serialize, Clone, Copy)]
pub struct F64(f64);

impl F64 {
    pub fn new(x: f64) -> Self {
        Self(x)
    }

    pub fn zero() -> Self {
        Self::new(0.0)
    }

    /// Rounds the inner `f64` with 1 decimal place.
    pub fn round(&self) -> String {
        format!("{:.1}", self.0)
    }

    pub fn value(&self) -> f64 {
        self.0
    }
}

impl std::ops::Add for F64 {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl std::ops::AddAssign for F64 {
    fn add_assign(&mut self, other: Self) {
        *self = Self(self.0 + other.0);
    }
}

impl std::ops::Sub for F64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

// based on: https://github.com/reem/rust-ordered-float/ `cmp` implementation for `OrderedFloat`
impl Ord for F64 {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(other) {
            Some(ordering) => ordering,
            None => {
                if self.0.is_nan() {
                    if other.0.is_nan() {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

impl Eq for F64 {}

impl fmt::Debug for F64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
