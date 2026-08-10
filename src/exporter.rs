//! Exporter — the dashboard-output component.
//!
//! On every `advanced` signal (a batch just landed), it reloads the position's
//! event streams, reads the *current* liquidity index and head timestamp, folds
//! them into an accrual series, and writes `headline.json` + `series.json` into
//! `export_dir`.

use std::sync::Arc;

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::BlockTransactionsKind;
use log::{debug, info, warn};
use serde::Serialize;
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::{
    select,
    sync::broadcast,
};

use crate::{
    config::Config,
    connections::Rpc,
    contracts::ISparkPool,
    error::CircuitBreakerEvent,
    model,
    position::Position,
    rpc::{Retryable, with_retry},
    types::{BASE_RATE_SPREAD, SeriesPoint},
};

const LOG_TARGET: &str = "Exporter";

#[derive(Debug, Error)]
pub enum ExporterError {
    #[error("model error: {0}")]
    Model(#[from] model::ModelError),
    #[error("rpc transport error: {0}")]
    Rpc(#[from] alloy::transports::TransportError),
    #[error("contract call error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("on-chain value exceeds u128")]
    Overflow,
    #[error("head block header not found for block {0}")]
    BlockNotFound(u64),
    #[error("invalid address in position config: {0}")]
    Address(String),
    #[error("json serialize error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("circuit-breaker send error: {0}")]
    CbBroadcast(#[from] broadcast::error::SendError<CircuitBreakerEvent>),
    #[error("broadcast receive error: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

impl Retryable for ExporterError {
    fn is_transient(&self) -> bool {
        matches!(self, ExporterError::Rpc(_) | ExporterError::Contract(_))
    }
}

/// Current-parameter inputs behind the headline margin.
#[derive(Debug, Serialize)]
struct Inputs {
    supply_apy: f64,
    util: f64,
    /// sUSDS savings rate APY (base_rate minus the off-chain spread).
    ssr: f64,
    /// Osero's borrow base rate = ssr + BASE_RATE_SPREAD.
    base_rate: f64,
}

/// How far each single input must move to reach a zero instantaneous margin,
/// holding the others fixed. All derived from the current segment's inputs; these
/// are first-order sensitivities, exactness is validated later against live data.
#[derive(Debug, Serialize)]
struct BreakEven {
    /// bps the SSR must *fall* to close the margin: −margin_bps / util (a
    /// ΔSSR moves cost by util·ΔSSR, so util·ΔSSR must offset the deficit).
    ssr_bps: f64,
    /// bps the supply APY must *rise* to close the margin: −margin_bps (supply
    /// enters revenue at unit weight, so it must cover the deficit 1:1).
    supply_bps: f64,
    /// Utilization (as a %) at which util·base_rate == supply_apy, i.e. the
    /// break-even utilization = supply_apy / base_rate.
    util_pct: f64,
}

#[derive(Debug, Serialize)]
struct Headline {
    as_of_block: u64,
    as_of_ts: u64,
    /// True when the instantaneous margin is positive (Osero earning the carry).
    making_money: bool,
    net_margin_bps: f64,
    cum_revenue: f64,
    cum_cost: f64,
    cum_net: f64,
    inputs: Inputs,
    break_even: BreakEven,
}

fn parse_addr(s: &str) -> Result<Address, ExporterError> {
    s.parse::<Address>()
        .map_err(|_| ExporterError::Address(s.to_string()))
}

fn to_u128(x: U256) -> Result<u128, ExporterError> {
    u128::try_from(x).map_err(|_| ExporterError::Overflow)
}

pub struct Exporter;

impl Exporter {
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        config: Arc<Config>,
        http: Rpc,
        pool: SqlitePool,
        position: Arc<Position>,
        position_id: i64,
        mut advanced_rx: broadcast::Receiver<u64>,
        circuit_breaker_sender: broadcast::Sender<CircuitBreakerEvent>,
        mut circuit_breaker_receiver: broadcast::Receiver<CircuitBreakerEvent>,
    ) -> Result<CircuitBreakerEvent, ExporterError> {
        info!(target: LOG_TARGET, "Starting");

        loop {
            debug!(target: LOG_TARGET, "Ping");

            select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting due to a circuit breaker event {cb_event:?}");
                    return Ok(cb_event?);
                },

                result = advanced_rx.recv() => match result {
                    Ok(advanced_block) => {
                        info!(target: LOG_TARGET, "Regenerating export (advanced to {advanced_block})");
                        if let Err(why) = export(&config, &http, &pool, &position, position_id).await {
                            warn!(target: LOG_TARGET, "Export failed {why:?}, exiting");
                            circuit_breaker_sender.send(CircuitBreakerEvent::ExporterFailure)?;
                            return Ok(CircuitBreakerEvent::ExporterFailure);
                        }
                    }
                    // Lagged: a newer export supersedes the missed one; loop on.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(target: LOG_TARGET, "advanced lagged by {n}, continuing");
                    }
                    // Closed: ingest gone → coordinated shutdown.
                    Err(broadcast::error::RecvError::Closed) => {
                        warn!(target: LOG_TARGET, "advanced channel closed, shutting down");
                        circuit_breaker_sender.send(CircuitBreakerEvent::ExporterFailure)?;
                        return Ok(CircuitBreakerEvent::ExporterFailure);
                    }
                }
            }
        }
    }
}

/// Load events, read the live index + head timestamp, fold, and write the JSON.
async fn export(
    config: &Config,
    http: &Rpc,
    pool: &SqlitePool,
    position: &Position,
    position_id: i64,
) -> Result<(), ExporterError> {
    let usds = parse_addr(&position.usds)?;
    let venue_pool = parse_addr(&position.venue_pool)?;

    let supplies = model::load_supply_events(pool, position_id).await?;
    let ssrs = model::load_ssr_changes(pool).await?;
    let reserves = model::load_reserve_updates(pool, position_id).await?;

    // Present-moment snapshot: head block, its timestamp, and the live index.
    // All three reads go through the shared paced/retried RPC path so a single
    // free-tier 429 during export doesn't trip ExporterFailure.
    let head = with_retry("get_block_number(head)", || async {
        http.get_block_number().await.map_err(ExporterError::from)
    })
    .await?;
    let head_block = with_retry("get_block_by_number(head)", || async {
        http.get_block_by_number(BlockNumberOrTag::Number(head), BlockTransactionsKind::Hashes)
            .await
            .map_err(ExporterError::from)
    })
    .await?
    .ok_or(ExporterError::BlockNotFound(head))?;
    let now_ts = head_block.header.timestamp;

    let pool_c = ISparkPool::new(venue_pool, http.clone());
    let live_index_ray = to_u128(
        with_retry("getReserveNormalizedIncome(head)", || async {
            pool_c
                .getReserveNormalizedIncome(usds)
                .block(BlockId::from(head))
                .call()
                .await
                .map_err(ExporterError::from)
        })
        .await?
        ._0,
    )?;

    let series = crate::accrual::fold(&supplies, &ssrs, &reserves, now_ts, live_index_ray);

    let headline = build_headline(&series, head, now_ts);

    // Write both files (create export_dir if missing).
    std::fs::create_dir_all(&config.export_dir)?;
    let headline_path = config.export_dir.join("headline.json");
    let series_path = config.export_dir.join("series.json");
    std::fs::write(&headline_path, serde_json::to_vec_pretty(&headline)?)?;
    std::fs::write(&series_path, serde_json::to_vec_pretty(&series)?)?;

    info!(
        target: LOG_TARGET,
        "Wrote {} ({} series points)", headline_path.display(), series.len()
    );
    Ok(())
}

/// Assemble the headline from the final series point (the "now" segment). With no
/// supply history yet the series is empty → a zeroed, not-making-money headline.
fn build_headline(series: &[SeriesPoint], as_of_block: u64, as_of_ts: u64) -> Headline {
    let Some(last) = series.last() else {
        return Headline {
            as_of_block,
            as_of_ts,
            making_money: false,
            net_margin_bps: 0.0,
            cum_revenue: 0.0,
            cum_cost: 0.0,
            cum_net: 0.0,
            inputs: Inputs { supply_apy: 0.0, util: 0.0, ssr: 0.0, base_rate: 0.0 },
            break_even: BreakEven { ssr_bps: 0.0, supply_bps: 0.0, util_pct: 0.0 },
        };
    };

    let margin_bps = last.inst_margin_bps;
    // base_rate = ssr_apy + BASE_RATE_SPREAD → recover the SSR component.
    let ssr = last.base_rate - BASE_RATE_SPREAD;

    // Break-even sensitivities (see BreakEven docs). Guard divisions.
    let ssr_bps = if last.util > 0.0 { -margin_bps / last.util } else { 0.0 };
    let supply_bps = -margin_bps;
    let util_pct = if last.base_rate > 0.0 {
        (last.supply_apy / last.base_rate) * 100.0
    } else {
        0.0
    };

    Headline {
        as_of_block: last.block.max(as_of_block),
        as_of_ts: as_of_ts.max(last.ts),
        making_money: margin_bps > 0.0,
        net_margin_bps: margin_bps,
        cum_revenue: last.cum_revenue,
        cum_cost: last.cum_cost,
        cum_net: last.cum_net,
        inputs: Inputs {
            supply_apy: last.supply_apy,
            util: last.util,
            ssr,
            base_rate: last.base_rate,
        },
        break_even: BreakEven { ssr_bps, supply_bps, util_pct },
    }
}
