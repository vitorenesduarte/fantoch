use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialOrd, Deserialize, Serialize, Clone, Copy)]
pub struct F64(f64);

impl F64 {
    pub fn new(x: f64) -> Self {
        Self(x)
    }

    pub fn zero() -> Self {
        Self::new(0.0)
    }

    pub fn nan() -> Self {
        Self::new(std::f64::NAN)
    }

    /// Rounds the inner `f64` with 1 decimal place.
    pub fn round(self) -> String {
        format!("{:.1}", self.0)
    }

    pub fn value(self) -> f64 {
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

impl std::ops::Mul for F64 {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        Self(self.0 * other.0)
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

impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() {
            other.0.is_nan()
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for F64 {}

impl fmt::Debug for F64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_with_zero() {
        assert_eq!(F64::new(0.0), F64::zero());
    }

    #[test]
    fn value() {
        assert_eq!(F64::zero().value(), 0.0);
        assert_eq!(F64::new(11.2).value(), 11.2);
    }

    #[test]
    fn add() {
        use std::ops::Add;

        let x = F64::new(11.2);
        let y = F64::new(30.8);
        let expected = F64::new(42.0);

        let result = x.add(y);
        assert_eq!(result, expected);
    }

    #[test]
    fn add_assign() {
        use std::ops::AddAssign;

        let mut x = F64::new(11.2);
        let y = F64::new(30.8);
        let expected = F64::new(42.0);

        x.add_assign(y);
        assert_eq!(x, expected);
    }

    #[test]
    fn sub() {
        use std::ops::Sub;

        let x = F64::new(10.8);
        let y = F64::new(30.8);
        let expected = F64::new(-20.0);

        let result = x.sub(y);
        assert_eq!(result, expected);
    }

    #[test]
    fn mul() {
        use std::ops::Mul;

        let x = F64::new(5.2);
        let y = F64::new(5.3);
        let expected = F64::new(27.56);

        let result = x.mul(y);
        assert_eq!(result, expected);
    }

    #[test]
    fn ord() {
        use std::cmp::Ordering;
        assert_eq!(F64::new(5.2).cmp(&F64::new(5.3)), Ordering::Less);
        assert_eq!(F64::new(5.3).cmp(&F64::new(5.2)), Ordering::Greater);
        assert_eq!(F64::new(5.2).cmp(&F64::new(5.2)), Ordering::Equal);
        assert_eq!(F64::new(5.2).cmp(&F64::nan()), Ordering::Less);
        assert_eq!(F64::nan().cmp(&F64::new(5.3)), Ordering::Greater);
        assert_eq!(F64::nan().cmp(&F64::nan()), Ordering::Equal);
    }
}
