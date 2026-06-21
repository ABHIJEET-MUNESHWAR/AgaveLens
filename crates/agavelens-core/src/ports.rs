//! Outbound ports — the interfaces the analytics core needs from the outside.
//!
//! Adapters live in `agavelens-infra`; the core depends only on these traits
//! (dependency-inversion). The [`Clock`](crate::Clock) port lives in
//! [`crate::guard`] alongside its implementations.

use async_trait::async_trait;

use agavelens_types::{Epoch, SlotSample, ValidatorId};

use crate::error::PortError;

/// Persistence for slot samples.
///
/// Implementations enforce a bounded capacity (oldest-evicted) so the store
/// cannot grow without limit.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SampleRepository: Send + Sync {
    /// Append a batch, evicting oldest beyond capacity. Returns the total
    /// number of samples retained after the operation.
    async fn save_batch(&self, samples: &[SlotSample]) -> Result<usize, PortError>;

    /// All retained samples, oldest first.
    async fn all(&self) -> Result<Vec<SlotSample>, PortError>;

    /// Samples led by a specific validator.
    async fn by_validator(&self, validator: &ValidatorId) -> Result<Vec<SlotSample>, PortError>;

    /// Samples belonging to a specific epoch.
    async fn by_epoch(&self, epoch: Epoch) -> Result<Vec<SlotSample>, PortError>;

    /// Number of retained samples.
    async fn count(&self) -> Result<usize, PortError>;
}
