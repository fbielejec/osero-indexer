//! Shared RPC pacing + retry, used by every chain-reading component.
//!
//! A single free-tier RPC endpoint enforces a credits-per-second ceiling. Every
//! component that touches the chain (the Listener's backfill reads AND the
//! Exporter's head reads) must funnel through the SAME process-wide pacing gate
//! and the SAME retry-with-backoff, or an un-paced call trips a 429 that a single
//! component would otherwise escalate to a circuit-break.
//!
//! `with_retry` is generic over the component error type via [`Retryable`]: each
//! component implements the trait for its own error so the shared loop can tell a
//! transient transport/contract failure (worth retrying) from a deterministic one.

use log::warn;
use tokio::time::{Duration, sleep};

const LOG_TARGET: &str = "Rpc";

/// Max attempts for a transient RPC failure before giving up (and circuit-breaking).
pub(crate) const MAX_RPC_RETRIES: u32 = 8;
/// Base backoff between RPC retries; doubles each attempt.
pub(crate) const RETRY_BASE_DELAY_MS: u64 = 400;
/// Minimum spacing between the *start* of consecutive RPC requests. A single
/// free-tier endpoint enforces a credits-per-second ceiling; firing the next
/// request the instant the previous one returns blows straight past it and every
/// call thrashes through several 429 retries. Pacing keeps us under the ceiling so
/// most calls succeed on the first attempt. Tunable via `OSERO_RPC_INTERVAL_MS`.
const DEFAULT_RPC_INTERVAL_MS: u64 = 400;

/// Implemented by component error types so [`with_retry`] can classify failures.
pub(crate) trait Retryable {
    /// True for transient failures worth retrying (transport/contract, notably a
    /// 429). Deterministic errors (decode/overflow/meta) return false.
    fn is_transient(&self) -> bool;
}

/// Global pacing gate shared by every RPC call in this process. Holds the instant
/// the last request was released; a caller waits until `last + interval`.
fn rate_gate() -> &'static tokio::sync::Mutex<tokio::time::Instant> {
    static GATE: std::sync::OnceLock<tokio::sync::Mutex<tokio::time::Instant>> =
        std::sync::OnceLock::new();
    GATE.get_or_init(|| tokio::sync::Mutex::new(tokio::time::Instant::now() - Duration::from_secs(1)))
}

fn rpc_interval() -> Duration {
    let ms = std::env::var("OSERO_RPC_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RPC_INTERVAL_MS);
    Duration::from_millis(ms)
}

/// Block until at least `rpc_interval()` has elapsed since the last released call,
/// then record now as the new release instant.
pub(crate) async fn pace() {
    let mut last = rate_gate().lock().await;
    let earliest = *last + rpc_interval();
    let now = tokio::time::Instant::now();
    if now < earliest {
        sleep(earliest - now).await;
    }
    *last = tokio::time::Instant::now();
}

/// Retry a fallible RPC operation on transient transport/contract errors — notably
/// HTTP 429 (Too Many Requests) throttling — with exponential backoff. Non-transient
/// errors are deterministic and propagate immediately.
///
/// This is what lets the daemon survive a single free-tier RPC endpoint: without
/// it, one 429 trips the circuit breaker and the supervisor reboot-loops, which
/// hammers the endpoint and keeps it throttled — a death spiral.
pub(crate) async fn with_retry<T, E, F, Fut>(what: &str, mut op: F) -> Result<T, E>
where
    E: Retryable + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt = 0u32;
    loop {
        pace().await;
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !e.is_transient() || attempt >= MAX_RPC_RETRIES {
                    return Err(e);
                }
                let delay = RETRY_BASE_DELAY_MS * 2u64.pow(attempt);
                warn!(
                    target: LOG_TARGET,
                    "{what} transient failure (attempt {}/{MAX_RPC_RETRIES}): {e:?}; retrying in {delay}ms",
                    attempt + 1,
                );
                sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}
