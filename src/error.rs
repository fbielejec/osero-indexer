//! Shared circuit-breaker signal broadcast across every runtime component.
//!
//! A single `broadcast::channel::<CircuitBreakerEvent>` is shared by all four
//! components. When any component hits an unrecoverable-in-place error it sends
//! its variant, and every other component's `select!` loop observes it and exits
//! gracefully (returning `Ok(event)`), so the supervisor can reboot the whole set
//! together rather than leaving orphaned tasks.

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum CircuitBreakerEvent {
    CursorFailure,
    ListenerFailure,
    IngestFailure,
    ExporterFailure,
}
