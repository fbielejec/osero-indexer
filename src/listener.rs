//! Listener — the core chain-reading component.
//!
//! Mirrors `event-listener/listener.rs`: a `select!` loop that, per iteration,
//! (1) receives the next unprocessed block from the cursor, (2) waits until that
//! block is finalized (`head − confirmations`), (3) `getLogs` over a
//! `sync_step`-bounded range for the four sources, (4) decodes + enriches each
//! log into a domain row (block timestamps + archive state reads), (5) ships an
//! [`EventsBatch`] to [`crate::ingest`] and waits for its ack before advancing.
//!
//! Everything runs over the HTTP archive provider — including head polling — so
//! the daemon needs only a single archive URL (WS stays unused for this task).

use std::{cmp::min, collections::HashMap, sync::Arc};

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{BlockTransactionsKind, Filter};
use alloy::sol_types::SolEvent;
use log::{debug, info, warn};
use thiserror::Error;
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot},
    time::{sleep, Duration},
};

use crate::{
    config::Config,
    connections::Rpc,
    contracts::{IAllocatorVault, ISUsds, ISparkPool, ISpUsds, IUsds, IVariableDebtUsds},
    error::CircuitBreakerEvent,
    position::Position,
    rpc::{Retryable, with_retry},
    types::{DebtEvent, DebtKind, ReserveUpdate, SsrChange, SupplyEvent, SupplyKind, ssr_ray_to_apy},
};

pub const BLOCK_PROD_TIME_SEC: u64 = 12;
const LOG_TARGET: &str = "Listener";

/// A finalized `[from_block, to_block]` range of decoded, enrichment-complete
/// events plus the per-insert side-values `model::ingest_batch` needs. `ack` is
/// fulfilled by Ingest once the batch is durably committed, which releases the
/// listener to advance.
pub struct EventsBatch {
    pub from_block: u64,
    pub to_block: u64,
    pub debts: Vec<DebtEvent>,
    pub supplies: Vec<SupplyEvent>,
    /// `(row, ssr_ray)`.
    pub ssrs: Vec<(SsrChange, u128)>,
    /// `(row, borrow_rate_ray, total_supplied, total_borrowed)`.
    pub reserves: Vec<(ReserveUpdate, u128, u128, u128)>,
    pub ack: oneshot::Sender<()>,
}

#[derive(Debug, Error)]
pub enum ListenerError {
    #[error("rpc transport error: {0}")]
    Rpc(#[from] alloy::transports::TransportError),
    #[error("contract call error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("log decode error: {0}")]
    Decode(#[from] alloy::sol_types::Error),
    #[error("on-chain value exceeds u128")]
    Overflow,
    #[error("log missing block/index metadata")]
    MissingLogMeta,
    #[error("block header not found for block {0}")]
    BlockNotFound(u64),
    #[error("invalid address in position config: {0}")]
    Address(String),
    #[error("events channel receiver dropped")]
    Send,
    #[error("circuit-breaker send error: {0}")]
    CbBroadcast(#[from] broadcast::error::SendError<CircuitBreakerEvent>),
    #[error("broadcast receive error: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

impl Retryable for ListenerError {
    fn is_transient(&self) -> bool {
        matches!(self, ListenerError::Rpc(_) | ListenerError::Contract(_))
    }
}

/// Left-pad-free bytes32 encoding of a short ASCII string (MakerDAO/Sky `file`
/// key encoding): the bytes sit at the front, zero-padded on the right.
fn bytes32_str(s: &str) -> B256 {
    let mut b = [0u8; 32];
    let bytes = s.as_bytes();
    let n = bytes.len().min(32);
    b[..n].copy_from_slice(&bytes[..n]);
    B256::from(b)
}

fn to_u128(x: U256) -> Result<u128, ListenerError> {
    u128::try_from(x).map_err(|_| ListenerError::Overflow)
}

fn parse_addr(s: &str) -> Result<Address, ListenerError> {
    s.parse::<Address>()
        .map_err(|_| ListenerError::Address(s.to_string()))
}

/// The four decoded event streams for one range, ready for `ingest_batch`.
struct Collected {
    debts: Vec<DebtEvent>,
    supplies: Vec<SupplyEvent>,
    ssrs: Vec<(SsrChange, u128)>,
    reserves: Vec<(ReserveUpdate, u128, u128, u128)>,
}

impl Collected {
    fn total(&self) -> usize {
        self.debts.len() + self.supplies.len() + self.ssrs.len() + self.reserves.len()
    }
}

/// Parsed, reused per-position addresses.
struct Addrs {
    vault: Address,
    usds: Address,
    susds: Address,
    venue_pool: Address,
    venue_atoken: Address,
    venue_vdebt: Address,
    proxy: Address,
}

pub struct Listener;

impl Listener {
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        config: Arc<Config>,
        http: Rpc,
        position: Arc<Position>,
        events_tx: mpsc::Sender<EventsBatch>,
        mut next_unprocessed_rx: broadcast::Receiver<u64>,
        last_processed_tx: broadcast::Sender<u64>,
        circuit_breaker_sender: broadcast::Sender<CircuitBreakerEvent>,
        mut circuit_breaker_receiver: broadcast::Receiver<CircuitBreakerEvent>,
    ) -> Result<CircuitBreakerEvent, ListenerError> {
        let confirmations = config.confirmations;
        let sync_step = config.sync_step;

        info!(target: LOG_TARGET, "Starting");

        // Parse the position addresses once. A bad address is a config error we
        // can't run with → circuit-break.
        let addrs = match (
            parse_addr(&position.vault),
            parse_addr(&position.usds),
            parse_addr(&position.susds),
            parse_addr(&position.venue_pool),
            parse_addr(&position.venue_atoken),
            parse_addr(&position.venue_vdebt),
            parse_addr(&position.proxy),
        ) {
            (Ok(vault), Ok(usds), Ok(susds), Ok(venue_pool), Ok(venue_atoken), Ok(venue_vdebt), Ok(proxy)) => {
                Addrs { vault, usds, susds, venue_pool, venue_atoken, venue_vdebt, proxy }
            }
            _ => {
                warn!(target: LOG_TARGET, "Exiting: position config has an invalid address");
                circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                return Ok(CircuitBreakerEvent::ListenerFailure);
            }
        };

        loop {
            debug!(target: LOG_TARGET, "Ping");

            // 1. Next unprocessed block from the cursor.
            let recv = select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting before next block due to circuit breaker {cb_event:?}");
                    return Ok(cb_event?);
                },
                r = next_unprocessed_rx.recv() => r,
            };
            let n = match recv {
                Ok(n) => n,
                // Lagged: loop to receive the latest next-unprocessed block.
                Err(broadcast::error::RecvError::Lagged(cnt)) => {
                    warn!(target: LOG_TARGET, "next_unprocessed lagged by {cnt}, continuing");
                    continue;
                }
                // Closed: coordinator gone → coordinated shutdown.
                Err(broadcast::error::RecvError::Closed) => {
                    warn!(target: LOG_TARGET, "next_unprocessed channel closed, shutting down");
                    circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                    return Ok(CircuitBreakerEvent::ListenerFailure);
                }
            };

            // 2. Wait until N is finalized (head − confirmations).
            let finalized = select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting while awaiting finality due to circuit breaker {cb_event:?}");
                    return Ok(cb_event?);
                },
                res = get_next_finalized_block_number(&http, confirmations, n) => match res {
                    Ok(f) => f,
                    Err(why) => {
                        warn!(target: LOG_TARGET, "Finality query failed {why:?}, exiting");
                        circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                        return Ok(CircuitBreakerEvent::ListenerFailure);
                    }
                },
            };

            // 3. Bound the range to `sync_step`.
            let to_block = min(finalized, n + sync_step - 1);
            info!(target: LOG_TARGET, "Processing blocks {n} - {to_block}");

            // 4. getLogs + decode + enrich.
            let collected = select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting while collecting due to circuit breaker {cb_event:?}");
                    return Ok(cb_event?);
                },
                res = collect_range(&http, &addrs, n, to_block, position.inception_block) => match res {
                    Ok(c) => c,
                    Err(why) => {
                        warn!(target: LOG_TARGET, "Range collection failed {why:?}, exiting");
                        circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                        return Ok(CircuitBreakerEvent::ListenerFailure);
                    }
                },
            };

            // 5. Empty range → advance without shipping a batch.
            if collected.total() == 0 {
                info!(target: LOG_TARGET, "No events in range, advancing to {}", to_block + 1);
                // A dropped receiver (Cursor gone) is a benign shutdown, not fatal:
                // circuit-break so the supervisor reboots the whole set.
                if last_processed_tx.send(to_block + 1).is_err() {
                    warn!(target: LOG_TARGET, "no cursor listening; shutting down for reboot");
                    circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                    return Ok(CircuitBreakerEvent::ListenerFailure);
                }
                continue;
            }

            // 6. Ship the batch and wait for the durable ack before advancing.
            let (ack_tx, ack_rx) = oneshot::channel::<()>();
            info!(
                target: LOG_TARGET,
                "Sending batch: {} debt / {} supply / {} ssr / {} reserve",
                collected.debts.len(), collected.supplies.len(),
                collected.ssrs.len(), collected.reserves.len()
            );
            events_tx
                .send(EventsBatch {
                    from_block: n,
                    to_block,
                    debts: collected.debts,
                    supplies: collected.supplies,
                    ssrs: collected.ssrs,
                    reserves: collected.reserves,
                    ack: ack_tx,
                })
                .await
                .map_err(|_| ListenerError::Send)?;

            select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting before ack due to circuit breaker {cb_event:?}");
                    return Ok(cb_event?);
                },
                ack_result = ack_rx => {
                    if ack_result.is_ok() {
                        info!(target: LOG_TARGET, "Batch committed, advancing to {}", to_block + 1);
                        // Dropped receiver (Cursor gone) → graceful reboot, not fatal.
                        if last_processed_tx.send(to_block + 1).is_err() {
                            warn!(target: LOG_TARGET, "no cursor listening; shutting down for reboot");
                            circuit_breaker_sender.send(CircuitBreakerEvent::ListenerFailure)?;
                            return Ok(CircuitBreakerEvent::ListenerFailure);
                        }
                    }
                }
            }
        }
    }
}

/// Poll the HTTP head until `head − confirmations ≥ not_older_than`, then return
/// that finalized number. Mirrors the reference `get_next_finalized_block_number`.
async fn get_next_finalized_block_number(
    http: &Rpc,
    confirmations: u64,
    not_older_than: u64,
) -> Result<u64, ListenerError> {
    loop {
        let head = with_retry("get_block_number(finality)", || async {
            http.get_block_number().await.map_err(ListenerError::from)
        })
        .await?;
        let finalized = head.saturating_sub(confirmations);
        if finalized >= not_older_than {
            return Ok(finalized);
        }
        debug!(target: LOG_TARGET, "head={head} finalized={finalized} < {not_older_than}, waiting");
        sleep(Duration::from_secs(BLOCK_PROD_TIME_SEC)).await;
    }
}

/// Fetch (and cache) a block's timestamp from its header.
async fn block_ts(
    http: &Rpc,
    cache: &mut HashMap<u64, u64>,
    block: u64,
) -> Result<u64, ListenerError> {
    if let Some(ts) = cache.get(&block) {
        return Ok(*ts);
    }
    let b = with_retry("get_block_by_number", || async {
        http.get_block_by_number(BlockNumberOrTag::Number(block), BlockTransactionsKind::Hashes)
            .await
            .map_err(ListenerError::from)
    })
    .await?
    .ok_or(ListenerError::BlockNotFound(block))?;
    let ts = b.header.timestamp;
    cache.insert(block, ts);
    Ok(ts)
}

/// Run all four `getLogs` queries over `[from, to]`, decode each log, and enrich
/// with block timestamps + archive state reads.
async fn collect_range(
    http: &Rpc,
    a: &Addrs,
    from: u64,
    to: u64,
    inception: u64,
) -> Result<Collected, ListenerError> {
    let mut ts_cache: HashMap<u64, u64> = HashMap::new();

    // Archive-read contract instances (state at historical blocks).
    let pool_c = ISparkPool::new(a.venue_pool, http.clone());
    let atoken_c = ISpUsds::new(a.venue_atoken, http.clone());
    let vdebt_c = IVariableDebtUsds::new(a.venue_vdebt, http.clone());
    let susds_c = ISUsds::new(a.susds, http.clone());

    let ray = U256::from(10u64).pow(U256::from(27u64));

    let mut debts = Vec::new();
    let mut supplies = Vec::new();
    let mut ssrs = Vec::new();
    let mut reserves = Vec::new();

    // --- AllocatorVault Draw / Wipe (two signatures, two queries). ----------
    for (sig, kind) in [
        (IAllocatorVault::Draw::SIGNATURE_HASH, DebtKind::Draw),
        (IAllocatorVault::Wipe::SIGNATURE_HASH, DebtKind::Wipe),
    ] {
        let filter = Filter::new()
            .address(a.vault)
            .event_signature(sig)
            .from_block(from)
            .to_block(to);
        let logs = with_retry("get_logs(vault draw/wipe)", || async {
            http.get_logs(&filter).await.map_err(ListenerError::from)
        })
        .await?;
        for log in logs {
            let (block, log_index) = log_meta(&log)?;
            let ts = block_ts(http, &mut ts_cache, block).await?;
            // Draw and Wipe share the `(address sender, uint256 wad)` shape.
            let wad = match kind {
                DebtKind::Draw => IAllocatorVault::Draw::decode_log_data(log.data(), true)?.wad,
                DebtKind::Wipe => IAllocatorVault::Wipe::decode_log_data(log.data(), true)?.wad,
            };
            debts.push(DebtEvent { block, ts, log_index, kind, wad: to_u128(wad)? });
        }
    }

    // --- USDS Transfer filtered to proxy ↔ aToken. --------------------------
    {
        let filter = Filter::new()
            .address(a.usds)
            .event_signature(IUsds::Transfer::SIGNATURE_HASH)
            .from_block(from)
            .to_block(to);
        let logs = with_retry("get_logs(usds transfer)", || async {
            http.get_logs(&filter).await.map_err(ListenerError::from)
        })
        .await?;
        for log in logs {
            let ev = IUsds::Transfer::decode_log_data(log.data(), true)?;
            // proxy → aToken = Deposit; aToken → proxy = Withdraw; else ignore.
            let kind = if ev.from == a.proxy && ev.to == a.venue_atoken {
                SupplyKind::Deposit
            } else if ev.from == a.venue_atoken && ev.to == a.proxy {
                SupplyKind::Withdraw
            } else {
                continue;
            };
            let (block, log_index) = log_meta(&log)?;
            let ts = block_ts(http, &mut ts_cache, block).await?;
            // Liquidity index at this block values the scaled aToken balance.
            let index_ray = to_u128(
                with_retry("getReserveNormalizedIncome", || async {
                    pool_c
                        .getReserveNormalizedIncome(a.usds)
                        .block(BlockId::from(block))
                        .call()
                        .await
                        .map_err(ListenerError::from)
                })
                .await?
                ._0,
            )?;
            supplies.push(SupplyEvent {
                block,
                ts,
                log_index,
                kind,
                amount: to_u128(ev.value)?,
                index_ray,
            });
        }
    }

    // --- sUSDS File(what == "ssr"). -----------------------------------------
    {
        let ssr_key = bytes32_str("ssr");
        let filter = Filter::new()
            .address(a.susds)
            .event_signature(ISUsds::File::SIGNATURE_HASH)
            .topic1(ssr_key)
            .from_block(from)
            .to_block(to);
        let logs = with_retry("get_logs(susds file ssr)", || async {
            http.get_logs(&filter).await.map_err(ListenerError::from)
        })
        .await?;
        for log in logs {
            let ev = ISUsds::File::decode_log_data(log.data(), true)?;
            let (block, log_index) = log_meta(&log)?;
            let ts = block_ts(http, &mut ts_cache, block).await?;
            let ssr_ray = to_u128(ev.data)?;
            let ssr_apy = ssr_ray_to_apy(ssr_ray);
            ssrs.push((SsrChange { block, ts, log_index, ssr_apy }, ssr_ray));
        }
    }

    // --- Seed the SSR *in effect at inception*. ------------------------------
    // sUSDS `File(ssr)` events are rare: the one that set the prevailing rate
    // typically fired *before* our [inception, head] scan window (here at block
    // 25596101, ~5.3k blocks before inception). With no in-window File event,
    // `base_rate` would collapse to the off-chain spread alone and cost would be
    // wildly understated. Unlike utilization — whose `ReserveDataUpdated` fires on
    // every reserve interaction and so lands at inception — SSR needs an explicit
    // baseline. Read `ssr()` at the inception block once (on the range covering it)
    // and inject it as the starting SSR observation. Real in-window File(ssr)
    // events (if any) layer on top via their own later timestamps.
    if from <= inception && inception <= to {
        let ssr_ray = to_u128(
            with_retry("susds.ssr()@inception", || async {
                susds_c
                    .ssr()
                    .block(BlockId::from(inception))
                    .call()
                    .await
                    .map_err(ListenerError::from)
            })
            .await?
            ._0,
        )?;
        let ts = block_ts(http, &mut ts_cache, inception).await?;
        let ssr_apy = ssr_ray_to_apy(ssr_ray);
        ssrs.push((SsrChange { block: inception, ts, log_index: 0, ssr_apy }, ssr_ray));
    }

    // --- Spark Pool ReserveDataUpdated filtered to reserve == usds. ---------
    {
        let filter = Filter::new()
            .address(a.venue_pool)
            .event_signature(ISparkPool::ReserveDataUpdated::SIGNATURE_HASH)
            .topic1(a.usds.into_word())
            .from_block(from)
            .to_block(to);
        let logs = with_retry("get_logs(reserve data updated)", || async {
            http.get_logs(&filter).await.map_err(ListenerError::from)
        })
        .await?;
        for log in logs {
            let ev = ISparkPool::ReserveDataUpdated::decode_log_data(log.data(), true)?;
            let (block, log_index) = log_meta(&log)?;
            let ts = block_ts(http, &mut ts_cache, block).await?;

            let liq_rate_ray = to_u128(ev.liquidityRate)?;
            let borrow_rate_ray = to_u128(ev.variableBorrowRate)?;

            // Utilization from the venue's total supplied/borrowed at this block.
            let total_supplied = to_u128(
                with_retry("spUSDS.totalSupply", || async {
                    atoken_c.totalSupply().block(BlockId::from(block)).call().await
                        .map_err(ListenerError::from)
                })
                .await?
                ._0,
            )?;
            let total_borrowed = to_u128(
                with_retry("vDebt.totalSupply", || async {
                    vdebt_c.totalSupply().block(BlockId::from(block)).call().await
                        .map_err(ListenerError::from)
                })
                .await?
                ._0,
            )?;
            // util_ray = total_borrowed * RAY / total_supplied (U256 math to avoid
            // u128 overflow; guard div-by-zero for an empty reserve).
            let util_ray = if total_supplied == 0 {
                0u128
            } else {
                to_u128(U256::from(total_borrowed) * ray / U256::from(total_supplied))?
            };

            reserves.push((
                ReserveUpdate { block, ts, log_index, liq_rate_ray, util_ray },
                borrow_rate_ray,
                total_supplied,
                total_borrowed,
            ));
        }
    }

    Ok(Collected { debts, supplies, ssrs, reserves })
}

/// Extract `(block_number, log_index)`; both are `Some` for logs from a mined,
/// finalized range — a `None` here signals an RPC anomaly worth circuit-breaking.
fn log_meta(log: &alloy::rpc::types::Log) -> Result<(u64, u64), ListenerError> {
    let block = log.block_number.ok_or(ListenerError::MissingLogMeta)?;
    let log_index = log.log_index.ok_or(ListenerError::MissingLogMeta)?;
    Ok((block, log_index))
}
