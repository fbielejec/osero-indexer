//! Position config loaded from `position.toml`.
//!
//! Addresses are kept as their `0x…` strings here and parsed into
//! [`alloy::primitives::Address`] at the chain boundary (the listener/exporter),
//! so this module stays free of the chain crate and the file schema maps 1:1 to
//! the TOML keys.

use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    pub ilk: String,
    pub vault: String,
    pub buffer: String,
    pub proxy: String,
    pub venue_pool: String,
    pub venue_atoken: String,
    pub venue_vdebt: String,
    pub usds: String,
    pub susds: String,
    pub inception_block: u64,
}

#[derive(Debug, Error)]
pub enum PositionError {
    #[error("failed to read position file {0}: {1}")]
    Read(String, std::io::Error),
    #[error("failed to parse position TOML: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Position {
    /// Read and parse a position config from a TOML file.
    pub fn load(path: &Path) -> Result<Self, PositionError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| PositionError::Read(path.display().to_string(), e))?;
        Ok(toml::from_str(&raw)?)
    }
}
