pub const RAY: f64 = 1e27;
/// 1e18 — USDS/aToken wei per whole token. Money amounts are stored RAW in wei
/// (exact u128) and converted to USDS token units in the accrual fold by dividing
/// by WAD. `index_ray` is a RAY-scaled ratio and is NOT divided by WAD.
pub const WAD: f64 = 1e18;
pub const SECONDS_PER_YEAR: f64 = 365.0 * 86_400.0;
pub const BASE_RATE_SPREAD: f64 = 0.0020; // 20 bps, off-chain constant

/// Aave liquidityRate: per-second nominal in RAY → effective APY (per-second compounding).
pub fn apr_ray_to_apy(rate_ray: u128) -> f64 {
    let per_sec = rate_ray as f64 / RAY / SECONDS_PER_YEAR;
    (1.0 + per_sec).powf(SECONDS_PER_YEAR) - 1.0
}

/// sUSDS ssr(): per-second rate in RAY → APY.
pub fn ssr_ray_to_apy(ssr_ray: u128) -> f64 {
    (ssr_ray as f64 / RAY).powf(SECONDS_PER_YEAR) - 1.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebtKind {
    Draw,
    Wipe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupplyKind {
    Deposit,
    Withdraw,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DebtEvent {
    pub block: u64,
    pub ts: u64,
    pub log_index: u64,
    pub kind: DebtKind,
    pub wad: u128,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupplyEvent {
    pub block: u64,
    pub ts: u64,
    pub log_index: u64,
    pub kind: SupplyKind,
    /// RAW USDS wei (u128, exact) as stored in the DB. The accrual fold converts
    /// to USDS token units by dividing by WAD; keeping wei here avoids the
    /// `amount as f64` precision loss above 2^53 at the storage boundary.
    pub amount: u128,
    pub index_ray: u128,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SsrChange {
    pub block: u64,
    pub ts: u64,
    pub log_index: u64,
    pub ssr_apy: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReserveUpdate {
    pub block: u64,
    pub ts: u64,
    pub log_index: u64,
    pub liq_rate_ray: u128,
    pub util_ray: u128,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SeriesPoint {
    pub ts: u64,
    pub block: u64,
    pub position: f64,
    pub supply_apy: f64,
    pub util: f64,
    pub base_rate: f64,
    pub inst_margin_bps: f64,
    pub cum_revenue: f64,
    pub cum_cost: f64,
    pub cum_net: f64,
}
