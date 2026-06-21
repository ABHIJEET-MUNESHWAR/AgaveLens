//! Schema assembly and shared request context.

use std::sync::Arc;

use async_graphql::{EmptySubscription, Error, Schema};

use agavelens_core::AnalyticsEngine;

use crate::mutation::MutationRoot;
use crate::query::QueryRoot;

/// Per-process context shared with every resolver.
#[derive(Clone)]
pub struct ApiContext {
    /// The analytics engine driving all queries and the ingest mutation.
    pub engine: Arc<AnalyticsEngine>,
}

impl ApiContext {
    /// Create a context around a shared engine.
    pub fn new(engine: Arc<AnalyticsEngine>) -> Self {
        Self { engine }
    }
}

/// The concrete schema type (read queries + ingest mutation, no subscriptions).
pub type AgaveLensSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Build the executable schema, injecting `ctx` and applying depth/complexity
/// limits to bound the cost of any single query.
pub fn build_schema(ctx: ApiContext) -> AgaveLensSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(ctx)
        .limit_depth(12)
        .limit_complexity(256)
        .finish()
}

/// Convert any displayable error into an `async-graphql` error.
///
/// `async_graphql::Error` has no blanket `From<E: Display>`, so resolvers funnel
/// fallible calls through this helper.
pub(crate) fn to_err<E: std::fmt::Display>(e: E) -> Error {
    Error::new(e.to_string())
}
