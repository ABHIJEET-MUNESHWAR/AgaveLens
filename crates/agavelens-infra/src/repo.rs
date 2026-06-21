//! A bounded in-memory [`SampleRepository`] adapter.

use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::VecDeque;

use agavelens_core::{PortError, SampleRepository};
use agavelens_types::{Epoch, SlotSample, ValidatorId};

/// In-memory sample store with a fixed capacity.
///
/// Backed by a `VecDeque` so eviction of the oldest sample is O(1). When the
/// store is full, the oldest samples are dropped — memory is bounded regardless
/// of ingest volume.
pub struct MemorySampleRepository {
    samples: RwLock<VecDeque<SlotSample>>,
    capacity: usize,
}

impl MemorySampleRepository {
    /// Create a store retaining at most `capacity` samples (minimum 1).
    pub fn new(capacity: usize) -> Self {
        Self {
            samples: RwLock::new(VecDeque::new()),
            capacity: capacity.max(1),
        }
    }

    /// Current number of retained samples.
    pub fn len(&self) -> usize {
        self.samples.read().len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.samples.read().is_empty()
    }

    /// The configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[async_trait]
impl SampleRepository for MemorySampleRepository {
    async fn save_batch(&self, batch: &[SlotSample]) -> Result<usize, PortError> {
        let mut g = self.samples.write();
        for s in batch {
            g.push_back(s.clone());
        }
        while g.len() > self.capacity {
            g.pop_front();
        }
        Ok(g.len())
    }

    async fn all(&self) -> Result<Vec<SlotSample>, PortError> {
        Ok(self.samples.read().iter().cloned().collect())
    }

    async fn by_validator(&self, validator: &ValidatorId) -> Result<Vec<SlotSample>, PortError> {
        Ok(self
            .samples
            .read()
            .iter()
            .filter(|s| s.leader() == validator)
            .cloned()
            .collect())
    }

    async fn by_epoch(&self, epoch: Epoch) -> Result<Vec<SlotSample>, PortError> {
        Ok(self
            .samples
            .read()
            .iter()
            .filter(|s| s.epoch() == epoch)
            .cloned()
            .collect())
    }

    async fn count(&self) -> Result<usize, PortError> {
        Ok(self.samples.read().len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample(slot: u64, leader: &str, epoch: u64) -> SlotSample {
        SlotSample::new(
            slot.into(),
            Epoch(epoch),
            ValidatorId::new(leader).unwrap(),
            400,
            100,
            false,
            Utc::now(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn save_and_count() {
        let repo = MemorySampleRepository::new(10);
        assert!(repo.is_empty());
        let total = repo
            .save_batch(&[sample(1, "a", 0), sample(2, "b", 0)])
            .await
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(repo.len(), 2);
        assert_eq!(repo.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn evicts_oldest_past_capacity() {
        let repo = MemorySampleRepository::new(2);
        repo.save_batch(&[sample(1, "a", 0)]).await.unwrap();
        repo.save_batch(&[sample(2, "b", 0)]).await.unwrap();
        let total = repo.save_batch(&[sample(3, "c", 0)]).await.unwrap();
        assert_eq!(total, 2);
        let all = repo.all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].slot().value(), 2);
        assert_eq!(all[1].slot().value(), 3);
    }

    #[tokio::test]
    async fn filters_by_validator_and_epoch() {
        let repo = MemorySampleRepository::new(100);
        repo.save_batch(&[
            sample(1, "alice", 0),
            sample(2, "bob", 0),
            sample(3, "alice", 1),
        ])
        .await
        .unwrap();
        let alice = repo
            .by_validator(&ValidatorId::new("alice").unwrap())
            .await
            .unwrap();
        assert_eq!(alice.len(), 2);
        let epoch0 = repo.by_epoch(Epoch(0)).await.unwrap();
        assert_eq!(epoch0.len(), 2);
        let epoch1 = repo.by_epoch(Epoch(1)).await.unwrap();
        assert_eq!(epoch1.len(), 1);
    }

    #[tokio::test]
    async fn zero_capacity_is_clamped_to_one() {
        let repo = MemorySampleRepository::new(0);
        assert_eq!(repo.capacity(), 1);
        let total = repo
            .save_batch(&[sample(1, "a", 0), sample(2, "b", 0)])
            .await
            .unwrap();
        assert_eq!(total, 1);
    }
}
