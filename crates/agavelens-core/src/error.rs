//! Error types for the analytics core.

use thiserror::Error;

use agavelens_types::InvalidSample;

/// A failure originating from an outbound port (repository, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PortError {
    /// The dependency is temporarily unavailable.
    #[error("dependency unavailable: {0}")]
    Unavailable(String),

    /// The dependency did not respond in time.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// An unexpected internal failure.
    #[error("internal error: {0}")]
    Internal(String),
}

impl PortError {
    /// Whether retrying the operation could plausibly succeed.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::Timeout(_))
    }

    /// A stable, machine-readable code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => "unavailable",
            Self::Timeout(_) => "timeout",
            Self::Internal(_) => "internal",
        }
    }
}

/// The unified error type returned by [`crate::AnalyticsEngine`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoreError {
    /// A sample (or identifier) failed validation.
    #[error(transparent)]
    Invalid(#[from] InvalidSample),

    /// An outbound port failed.
    #[error(transparent)]
    Port(#[from] PortError),

    /// The submitted batch exceeded the configured maximum.
    #[error("batch too large: {size} samples (max {max})")]
    BatchTooLarge {
        /// Number of samples submitted.
        size: usize,
        /// Configured maximum batch size.
        max: usize,
    },

    /// Ingest was rejected by the rate limiter (back-pressure).
    #[error("ingest throttled by rate limiter")]
    Throttled,
}

impl CoreError {
    /// A stable, machine-readable code for logs, metrics, and API responses.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Invalid(e) => e.code(),
            Self::Port(e) => e.code(),
            Self::BatchTooLarge { .. } => "batch_too_large",
            Self::Throttled => "throttled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_retryability() {
        assert!(PortError::Unavailable("x".into()).is_retryable());
        assert!(PortError::Timeout("x".into()).is_retryable());
        assert!(!PortError::Internal("x".into()).is_retryable());
    }

    #[test]
    fn core_codes() {
        assert_eq!(CoreError::Throttled.code(), "throttled");
        assert_eq!(
            CoreError::BatchTooLarge { size: 5, max: 2 }.code(),
            "batch_too_large"
        );
        assert_eq!(
            CoreError::Port(PortError::Unavailable("db".into())).code(),
            "unavailable"
        );
        assert_eq!(
            CoreError::Invalid(InvalidSample::EmptyValidator).code(),
            "empty_validator"
        );
    }

    #[test]
    fn from_conversions() {
        let e: CoreError = InvalidSample::EmptyValidator.into();
        assert!(matches!(e, CoreError::Invalid(_)));
        let e: CoreError = PortError::Timeout("t".into()).into();
        assert!(matches!(e, CoreError::Port(_)));
    }
}
