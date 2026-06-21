//! # agavelens-core
//!
//! The analytics engine: outbound [`ports`](crate::SampleRepository), lightweight
//! resilience [`guard`]s (clock + rate limiter), configuration, and the
//! `rayon`-parallel [`aggregate`]or that turns slot samples into reports.
//!
//! Depends only on `agavelens-types` (plus async/parallel runtimes) — no web or
//! database frameworks (hexagonal: dependencies point inward).

#![forbid(unsafe_code)]

mod aggregate;
mod config;
mod engine;
mod error;
mod guard;
mod ports;

pub use aggregate::{aggregate, report_for, summarize_epoch};
pub use config::AnalyticsConfig;
pub use engine::{AnalyticsEngine, EngineDeps, IngestSummary};
pub use error::{CoreError, PortError};
pub use guard::{Clock, ManualClock, RateLimiter, SystemClock};
pub use ports::SampleRepository;
