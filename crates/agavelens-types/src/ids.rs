//! Strongly-typed identifiers for the analytics domain.
//!
//! Newtypes keep raw `u64`s and validator strings from being transposed at call
//! sites — a slot can never be silently used where an epoch is expected.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::InvalidSample;

/// Maximum accepted length, in bytes, of a validator identity string.
///
/// A base58-encoded Ed25519 pubkey is 44 characters; 64 leaves head-room for
/// alternative encodings without permitting unbounded input.
pub const MAX_VALIDATOR_ID_LEN: usize = 64;

/// A Solana slot number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Slot(pub u64);

impl Slot {
    /// The underlying slot number.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// The epoch this slot belongs to, given the cluster's epoch length.
    ///
    /// Returns epoch `0` when `slots_per_epoch` is `0` rather than dividing by
    /// zero — callers validate the schedule elsewhere.
    pub const fn epoch(self, slots_per_epoch: u64) -> Epoch {
        if slots_per_epoch == 0 {
            Epoch(0)
        } else {
            Epoch(self.0 / slots_per_epoch)
        }
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Slot {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// A Solana epoch number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    /// The underlying epoch number.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Epoch {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// A validator's identity (typically a base58 pubkey), validated to be
/// non-empty and bounded in length.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ValidatorId(String);

impl ValidatorId {
    /// Construct a validated identity.
    ///
    /// # Errors
    /// Returns [`InvalidSample::EmptyValidator`] for empty input or
    /// [`InvalidSample::ValidatorIdTooLong`] beyond [`MAX_VALIDATOR_ID_LEN`].
    pub fn new(id: impl Into<String>) -> Result<Self, InvalidSample> {
        let id = id.into();
        if id.is_empty() {
            return Err(InvalidSample::EmptyValidator);
        }
        if id.len() > MAX_VALIDATOR_ID_LEN {
            return Err(InvalidSample::ValidatorIdTooLong {
                len: id.len(),
                max: MAX_VALIDATOR_ID_LEN,
            });
        }
        Ok(Self(id))
    }

    /// Borrow the identity as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the identity, returning the owned string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ValidatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_epoch_division() {
        assert_eq!(Slot(0).epoch(432_000), Epoch(0));
        assert_eq!(Slot(431_999).epoch(432_000), Epoch(0));
        assert_eq!(Slot(432_000).epoch(432_000), Epoch(1));
        assert_eq!(Slot(864_001).epoch(432_000), Epoch(2));
    }

    #[test]
    fn slot_epoch_zero_schedule_is_safe() {
        assert_eq!(Slot(123).epoch(0), Epoch(0));
    }

    #[test]
    fn slot_and_epoch_value_and_display() {
        assert_eq!(Slot(42).value(), 42);
        assert_eq!(Slot::from(7).to_string(), "7");
        assert_eq!(Epoch::from(3).value(), 3);
        assert_eq!(Epoch(9).to_string(), "9");
    }

    #[test]
    fn validator_id_accepts_valid() {
        let v = ValidatorId::new("Vote111111111111111111111111111111111111111").unwrap();
        assert_eq!(v.as_str().len(), 43);
        assert_eq!(v.to_string(), "Vote111111111111111111111111111111111111111");
    }

    #[test]
    fn validator_id_rejects_empty() {
        assert_eq!(ValidatorId::new(""), Err(InvalidSample::EmptyValidator));
    }

    #[test]
    fn validator_id_rejects_too_long() {
        let long = "x".repeat(MAX_VALIDATOR_ID_LEN + 1);
        assert_eq!(
            ValidatorId::new(long),
            Err(InvalidSample::ValidatorIdTooLong {
                len: MAX_VALIDATOR_ID_LEN + 1,
                max: MAX_VALIDATOR_ID_LEN,
            })
        );
    }

    #[test]
    fn validator_id_into_inner_roundtrips() {
        let v = ValidatorId::new("alice").unwrap();
        assert_eq!(v.into_inner(), "alice");
    }
}
