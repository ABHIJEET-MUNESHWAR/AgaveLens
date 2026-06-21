//! Validation errors for analytics samples.

use thiserror::Error;

/// Reasons a [`crate::SlotSample`] (or one of its identifiers) is rejected at
/// construction time. Returned at the system boundary; never panics.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidSample {
    /// The validator identity string was empty.
    #[error("validator identity must not be empty")]
    EmptyValidator,

    /// The validator identity exceeded [`crate::MAX_VALIDATOR_ID_LEN`].
    #[error("validator identity too long: {len} bytes (max {max})")]
    ValidatorIdTooLong {
        /// Length supplied.
        len: usize,
        /// Maximum permitted length.
        max: usize,
    },

    /// A numeric metric exceeded its sane upper bound.
    #[error("{field} value {value} exceeds maximum {max}")]
    ValueOutOfRange {
        /// Name of the offending field.
        field: &'static str,
        /// Value supplied.
        value: u64,
        /// Maximum permitted value.
        max: u64,
    },
}

impl InvalidSample {
    /// A stable, machine-readable code for logs, metrics labels, and API responses.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyValidator => "empty_validator",
            Self::ValidatorIdTooLong { .. } => "validator_id_too_long",
            Self::ValueOutOfRange { .. } => "value_out_of_range",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable() {
        assert_eq!(InvalidSample::EmptyValidator.code(), "empty_validator");
        assert_eq!(
            InvalidSample::ValidatorIdTooLong { len: 80, max: 64 }.code(),
            "validator_id_too_long"
        );
        assert_eq!(
            InvalidSample::ValueOutOfRange {
                field: "slot_time_ms",
                value: 99,
                max: 10,
            }
            .code(),
            "value_out_of_range"
        );
    }

    #[test]
    fn display_is_human_readable() {
        let e = InvalidSample::ValueOutOfRange {
            field: "vote_latency_ms",
            value: 70_000,
            max: 60_000,
        };
        assert_eq!(
            e.to_string(),
            "vote_latency_ms value 70000 exceeds maximum 60000"
        );
    }
}
