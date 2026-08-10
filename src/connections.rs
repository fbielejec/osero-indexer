//! alloy 0.8 provider construction, split into an HTTP path (backfill `getLogs` +
//! archive state reads) and a WS path (head-follow subscriptions).
//!
//! Both constructors return the same concrete type [`Rpc`] so the listener can store
//! and pass either interchangeably. It is a [`RootProvider<BoxTransport>`]: the transport
//! is boxed so the HTTP (`Http<reqwest::Client>`) and WS (`PubSubFrontend`) providers
//! collapse to one nameable type. `RootProvider` implements [`Provider`], which supplies
//! both `.get_logs(&Filter)` and the `.call()` path used by the `#[sol(rpc)]` contract
//! bindings in [`crate::contracts`]. It is cheaply `Clone` (internally reference-counted),
//! so a single instance can be shared across tasks.

use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::transports::{BoxTransport, TransportError};
use thiserror::Error;

/// Unified provider handle used across the chain layer.
pub type Rpc = RootProvider<BoxTransport>;

#[derive(Debug, Error)]
pub enum ConnError {
    #[error("invalid rpc url: {0}")]
    Url(String),
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}

/// HTTP provider for backfill (`getLogs`) and archive state reads.
pub async fn http_provider(url: &str) -> Result<Rpc, ConnError> {
    let parsed = url.parse().map_err(|e| ConnError::Url(format!("{e}")))?;
    let provider = ProviderBuilder::new().on_http(parsed);
    Ok(provider.root().clone().boxed())
}

/// WebSocket provider for head-follow (block/log subscriptions).
pub async fn ws_provider(url: &str) -> Result<Rpc, ConnError> {
    let provider = ProviderBuilder::new().on_ws(WsConnect::new(url)).await?;
    Ok(provider.root().clone().boxed())
}
