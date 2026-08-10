//! Live smoke test — feature-gated, OFF by default.
//!
//! `#![cfg(feature = "live-smoke")]` makes the whole file inert unless built with
//! `--features live-smoke`, so the default `cargo test` (offline unit tests) never
//! touches the network. Run it with:
//!
//!   set -a; source ../.env; set +a
//!   cargo test --features live-smoke --test smoke -- --nocapture
//!
//! Design: instead of a slow, rate-limited full backfill, this reads live state at
//! a single PINNED, well-finalized block and reconciles it against the ground-truth
//! values from ../discovery/mechanics.md, then reproduces the headline P&L mechanic
//! (base-rate-on-utilization) end-to-end against real chain. This exercises the alloy
//! bindings, the archive-read path, and `types::*` conversions together.
//!
//! SECURITY: RPC content is untrusted data. The RPC URL/key is never printed.
#![cfg(feature = "live-smoke")]

use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, FixedBytes};

use osero_indexer::connections::http_provider;
use osero_indexer::contracts::{ISUsds, ISpUsds, ISparkPool, IVariableDebtUsds, IVat};
use osero_indexer::position::Position;
use osero_indexer::types::{BASE_RATE_SPREAD, apr_ray_to_apy, ssr_ray_to_apy};

/// Pinned, well-finalized mainnet block (after both Osero draws).
const PINNED_BLOCK: u64 = 25_682_000;

/// Sky/Maker Vat (MCD_VAT), from ../discovery/contracts.json. Not in position.toml,
/// so pinned here as a documented const for the reconciliation read.
const VAT: &str = "0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B";

fn parse_addr(s: &str) -> Address {
    s.parse::<Address>()
        .unwrap_or_else(|_| panic!("bad address in config: {s}"))
}

/// `ilk` name → right-zero-padded bytes32 (Sky's ilk key encoding).
fn ilk_to_bytes32(ilk: &str) -> FixedBytes<32> {
    let bytes = ilk.as_bytes();
    assert!(bytes.len() <= 32, "ilk name too long for bytes32");
    let mut out = [0u8; 32];
    out[..bytes.len()].copy_from_slice(bytes);
    FixedBytes::<32>::from(out)
}

/// Retry a chain read a few times with linear backoff, so an occasional free-tier
/// 429 doesn't flake the test (this test path has none of the daemon's pacing).
async fn retry<T, E, F, Fut>(label: &str, f: F) -> T
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) => {
                attempt += 1;
                assert!(attempt < 5, "{label} failed after {attempt} attempts: {e:?}");
                eprintln!("  [{label}] retry {attempt} after error: {e:?}");
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
    }
}

/// Small courtesy gap between reads to stay under free-tier rate limits.
async fn pace() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}

#[tokio::test]
async fn live_pinned_block_reconciliation_and_margin() {
    // Skip gracefully if the RPC URL is not provided (e.g. plain `cargo test
    // --features live-smoke` without sourcing .env). `make smoke` exports it.
    let Ok(rpc_url) = std::env::var("ETH_RPC_URL") else {
        eprintln!("ETH_RPC_URL unset — skipping live smoke test (run `make smoke`).");
        return;
    };

    let http = http_provider(&rpc_url)
        .await
        .expect("failed to build HTTP provider");

    let position = Position::load(std::path::Path::new("position.toml"))
        .expect("failed to load position.toml");

    let at = BlockId::from(PINNED_BLOCK);

    // Resolve addresses from the position config (+ the Vat const).
    let vat_addr = parse_addr(VAT);
    let vault_addr = parse_addr(&position.vault);
    let proxy_addr = parse_addr(&position.proxy);
    let usds_addr = parse_addr(&position.usds);
    let susds_addr = parse_addr(&position.susds);
    let pool_addr = parse_addr(&position.venue_pool);
    let atoken_addr = parse_addr(&position.venue_atoken);
    let vdebt_addr = parse_addr(&position.venue_vdebt);
    let ilk = ilk_to_bytes32(&position.ilk);

    let vat = IVat::new(vat_addr, http.clone());
    let susds = ISUsds::new(susds_addr, http.clone());
    let pool = ISparkPool::new(pool_addr, http.clone());
    let spusds = ISpUsds::new(atoken_addr, http.clone());
    let vdebt = IVariableDebtUsds::new(vdebt_addr, http.clone());

    println!("\n=== osero live smoke — pinned block {PINNED_BLOCK} (mainnet) ===");
    println!("ilk = {}", position.ilk);

    // --- 1. Debt (drawn USDS) = urns(ilk,vault).art * ilks(ilk).rate / 1e27 -------
    let urns = retry("vat.urns", || async {
        vat.urns(ilk, vault_addr).block(at).call().await
    })
    .await;
    pace().await;
    let ilks = retry("vat.ilks", || async { vat.ilks(ilk).block(at).call().await }).await;
    pace().await;

    let art = u128::try_from(urns.art).expect("art exceeds u128");
    let rate = u128::try_from(ilks.rate).expect("rate exceeds u128");
    // art is wad (1e18), rate is ray (1e27): debt_usds = art/1e18 * rate/1e27.
    let debt_usds = (art as f64 / 1e18) * (rate as f64 / 1e27);
    println!("\n[1] debt (drawn USDS)      = {debt_usds:>14.2} USDS");
    println!("    ilk.rate               = {:.18} RAY (1.0 => zero on-chain fee)", rate as f64 / 1e27);
    assert!(
        (1_000_000.0..=1_002_000.0).contains(&debt_usds),
        "debt {debt_usds} out of [1_000_000, 1_002_000]"
    );

    // --- 2. Position = spUSDS.balanceOf(proxy) (18 decimals) ----------------------
    let bal = retry("spusds.balanceOf", || async {
        spusds.balanceOf(proxy_addr).block(at).call().await
    })
    .await
    ._0;
    pace().await;
    let position_usds = u128::try_from(bal).expect("balance exceeds u128") as f64 / 1e18;
    println!("[2] position (spUSDS bal)  = {position_usds:>14.2} USDS");
    assert!(
        (1_000_000.0..=1_010_000.0).contains(&position_usds),
        "position {position_usds} out of [1_000_000, 1_010_000]"
    );

    // --- 3. Utilization = vDebt.totalSupply / spUSDS.totalSupply ------------------
    let vdebt_ts = retry("vdebt.totalSupply", || async {
        vdebt.totalSupply().block(at).call().await
    })
    .await
    ._0;
    pace().await;
    let spusds_ts = retry("spusds.totalSupply", || async {
        spusds.totalSupply().block(at).call().await
    })
    .await
    ._0;
    pace().await;
    let borrowed = u128::try_from(vdebt_ts).expect("vdebt total exceeds u128") as f64;
    let supplied = u128::try_from(spusds_ts).expect("spusds total exceeds u128") as f64;
    let util = borrowed / supplied;
    println!("[3] utilization            = {util:>14.6}  ({:.2}%)", util * 100.0);
    assert!(
        (0.55..=0.68).contains(&util),
        "utilization {util} out of [0.55, 0.68]"
    );

    // --- 4. Base rate = ssr_apy + 20bps (off-chain spread) ------------------------
    let ssr_ray = retry("susds.ssr", || async { susds.ssr().block(at).call().await })
        .await
        ._0;
    pace().await;
    let ssr = u128::try_from(ssr_ray).expect("ssr exceeds u128");
    let ssr_apy = ssr_ray_to_apy(ssr);
    let base_rate = ssr_apy + BASE_RATE_SPREAD;
    println!("[4] SSR APY                = {ssr_apy:>14.6}  ({:.4}%)", ssr_apy * 100.0);
    println!("    base rate (SSR+20bps)  = {base_rate:>14.6}  ({:.4}%)", base_rate * 100.0);
    assert!(
        (0.033..=0.037).contains(&ssr_apy),
        "ssr_apy {ssr_apy} out of [0.033, 0.037]"
    );
    assert!(
        (0.035..=0.039).contains(&base_rate),
        "base_rate {base_rate} out of [0.035, 0.039]"
    );

    // --- 5. Supply APY = apr_ray_to_apy(reserve.currentLiquidityRate) -------------
    let reserve = retry("pool.getReserveData", || async {
        pool.getReserveData(usds_addr).block(at).call().await
    })
    .await
    ._0;
    pace().await;
    let liq_rate_ray: u128 = reserve.currentLiquidityRate;
    let supply_apy = apr_ray_to_apy(liq_rate_ray);
    println!("[5] currentLiquidityRate   = {liq_rate_ray} RAY");
    println!("    supply APY             = {supply_apy:>14.6}  ({:.4}%)", supply_apy * 100.0);
    assert!(
        (0.018..=0.024).contains(&supply_apy),
        "supply_apy {supply_apy} out of [0.018, 0.024]"
    );

    // --- 6. The mechanic: instantaneous net margin bps ----------------------------
    // margin = (supply_apy − utilization·base_rate) · 1e4. Base rate is charged only
    // on the borrowed portion (util·position), so cost rate on position = util·base.
    let cost_rate = util * base_rate;
    let margin_bps = (supply_apy - cost_rate) * 1e4;
    println!("\n=== MECHANIC (headline) ===");
    println!("    revenue rate (supply)  = {:.4}%", supply_apy * 100.0);
    println!("    cost rate (util·base)  = {:.4}%", cost_rate * 100.0);
    println!("    net margin             = {margin_bps:>+8.2} bps  (negative => LOSING)");
    assert!(
        (-35.0..=-15.0).contains(&margin_bps),
        "margin {margin_bps} bps out of [-35.0, -15.0] — expected ≈ -25 bps"
    );
    println!(
        "\nOK: live reconciliation passed; margin ≈ {margin_bps:.1} bps reproduces the -25 bps finding.\n"
    );
}
