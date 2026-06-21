//! Lightweight resilience guards.
//!
//! AgaveLens is a read-mostly analytics service, so it deliberately carries a
//! *lighter* resilience kit than the streaming services in this workspace: a
//! monotonic [`Clock`] abstraction and a token-bucket [`RateLimiter`] that
//! applies back-pressure to ingest. Bounded memory (see
//! [`crate::AnalyticsConfig::max_samples`]) and a batch-size guard round it out.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;

/// A source of wall-clock time, injectable for deterministic tests.
#[cfg_attr(test, mockall::automock)]
pub trait Clock: Send + Sync {
    /// The current instant.
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock backed by the system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A manually-advanced clock for tests — no sleeping, fully deterministic.
#[derive(Debug, Clone)]
pub struct ManualClock {
    inner: Arc<Mutex<DateTime<Utc>>>,
}

impl ManualClock {
    /// Create a clock fixed at `start`.
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(start)),
        }
    }

    /// Advance the clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        let mut g = self.inner.lock();
        *g += delta;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.inner.lock()
    }
}

struct BucketState {
    tokens: f64,
    last: DateTime<Utc>,
}

/// A token-bucket rate limiter driven by an injected [`Clock`].
///
/// Tokens refill continuously at `refill_per_sec` up to `capacity`. Acquisition
/// is non-blocking: [`try_acquire`](Self::try_acquire) returns `false` rather
/// than waiting, so callers can shed load instead of queueing.
pub struct RateLimiter<C: Clock = SystemClock> {
    state: Mutex<BucketState>,
    capacity: f64,
    refill_per_sec: f64,
    clock: C,
}

impl<C: Clock> RateLimiter<C> {
    /// Create a limiter starting full (`capacity` tokens available).
    pub fn new(capacity: u32, refill_per_sec: f64, clock: C) -> Self {
        let now = clock.now();
        Self {
            state: Mutex::new(BucketState {
                tokens: f64::from(capacity),
                last: now,
            }),
            capacity: f64::from(capacity),
            refill_per_sec: refill_per_sec.max(0.0),
            clock,
        }
    }

    /// Try to take a single token.
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_n(1.0)
    }

    /// Try to take `n` tokens, refilling first based on elapsed time.
    pub fn try_acquire_n(&self, n: f64) -> bool {
        let now = self.clock.now();
        let mut st = self.state.lock();
        let elapsed_us = (now - st.last).num_microseconds().unwrap_or(0).max(0);
        let elapsed_secs = elapsed_us as f64 / 1_000_000.0;
        st.tokens = (st.tokens + elapsed_secs * self.refill_per_sec).min(self.capacity);
        st.last = now;
        if st.tokens >= n {
            st.tokens -= n;
            true
        } else {
            false
        }
    }

    /// Currently available tokens (after refilling to `now`). Primarily for tests.
    pub fn available(&self) -> f64 {
        let now = self.clock.now();
        let mut st = self.state.lock();
        let elapsed_us = (now - st.last).num_microseconds().unwrap_or(0).max(0);
        let elapsed_secs = elapsed_us as f64 / 1_000_000.0;
        st.tokens = (st.tokens + elapsed_secs * self.refill_per_sec).min(self.capacity);
        st.last = now;
        st.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn manual_clock_advances() {
        let c = ManualClock::new(epoch());
        assert_eq!(c.now(), epoch());
        c.advance(Duration::seconds(5));
        assert_eq!(c.now(), epoch() + Duration::seconds(5));
    }

    #[test]
    fn limiter_drains_then_refuses() {
        let clock = ManualClock::new(epoch());
        let rl = RateLimiter::new(3, 0.0, clock.clone());
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        // bucket empty, no refill
        assert!(!rl.try_acquire());
    }

    #[test]
    fn limiter_refills_over_time() {
        let clock = ManualClock::new(epoch());
        let rl = RateLimiter::new(10, 10.0, clock.clone());
        for _ in 0..10 {
            assert!(rl.try_acquire());
        }
        assert!(!rl.try_acquire());
        // 10 tokens/sec -> 1s yields full refill
        clock.advance(Duration::seconds(1));
        assert!((rl.available() - 10.0).abs() < 1e-6);
        assert!(rl.try_acquire());
    }

    #[test]
    fn limiter_caps_at_capacity() {
        let clock = ManualClock::new(epoch());
        let rl = RateLimiter::new(5, 1000.0, clock.clone());
        clock.advance(Duration::seconds(60));
        assert!((rl.available() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn system_clock_moves_forward() {
        let c = SystemClock;
        let a = c.now();
        let b = c.now();
        assert!(b >= a);
    }
}
