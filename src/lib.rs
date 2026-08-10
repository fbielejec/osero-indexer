//! Library surface for the osero-indexer crate.
//!
//! The daemon lives in `main.rs`, but the pure-core (types/accrual) and the
//! chain layer (connections/contracts) must also be reachable from integration
//! tests under `tests/`, which can only see the crate's PUBLIC lib API. This
//! module re-exposes every component module so both the binary (`main.rs`, via
//! `use osero_indexer::…`) and the live smoke test can import them.
//!
//! The offline "pure core" (types/accrual) is fully exercised by unit tests; some
//! model helpers and chain-layer paths are only reachable from the running daemon
//! (or the feature-gated smoke test), so allow dead_code to stay warning-clean.
#![allow(dead_code)]

pub mod accrual;
pub mod config;
pub mod connections;
pub mod contracts;
pub mod cursor;
pub mod error;
pub mod exporter;
pub mod ingest;
pub mod listener;
pub mod model;
pub mod position;
pub mod rpc;
pub mod types;
