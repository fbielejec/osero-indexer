use std::path::PathBuf;

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "osero-indexer")]
pub struct Config {
    /// HTTPS/HTTP mainnet RPC url (reads: backfill `getLogs` + archive state).
    #[arg(long, env = "ETH_RPC_URL")]
    pub rpc_url: String,
    /// HTTP archive RPC url (backfill + historical state reads).
    /// Falls back to `rpc_url` when unset (single-URL run).
    #[arg(long, env = "ETH_ARCHIVE_URL")]
    pub archive_url: Option<String>,
    /// Path to the position config
    #[arg(long, env, default_value = "position.toml")]
    pub position_path: PathBuf,
    /// SQLite database path
    #[arg(long, env, default_value = "osero.db")]
    pub db_path: String,
    /// Output dir for the dashboard JSON
    #[arg(long, env, default_value = "dist")]
    pub export_dir: PathBuf,
    /// Blocks per getLogs chunk during backfill
    #[arg(long, env, default_value = "5000")]
    pub sync_step: u64,
    /// Confirmations buffer before a block is considered final
    #[arg(long, env, default_value = "12")]
    pub confirmations: u64,
}

impl Config {
    /// The HTTP archive URL to use, falling back to `rpc_url` when unset.
    pub fn archive_url(&self) -> String {
        self.archive_url
            .clone()
            .unwrap_or_else(|| self.rpc_url.clone())
    }
}
