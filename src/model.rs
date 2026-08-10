use crate::types::*;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("failed to parse stored integer TEXT: {0}")]
    Parse(#[from] std::num::ParseIntError),
}

/// Open (creating if needed) a SQLite pool. Accepts a filesystem path
/// (e.g. "osero.db") or "sqlite::memory:". Foreign keys are enabled.
///
/// For in-memory databases the pool is pinned to a single connection: each
/// SQLite `:memory:` connection is a *separate* database, so a multi-connection
/// pool would run migrations against one throwaway DB and queries against
/// another. A single connection keeps the in-memory schema visible to all work.
pub async fn connect(db_path: &str) -> Result<SqlitePool, ModelError> {
    let is_memory = db_path.contains(":memory:");

    let opts = if is_memory {
        SqliteConnectOptions::from_str(db_path)?.foreign_keys(true)
    } else {
        // Strip an optional "sqlite:" scheme prefix for the raw filename form.
        let filename = db_path.strip_prefix("sqlite:").unwrap_or(db_path);
        SqliteConnectOptions::new()
            .filename(filename)
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_secs(5))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
    };

    let pool = SqlitePoolOptions::new()
        .max_connections(if is_memory { 1 } else { 5 })
        .connect_with(opts)
        .await?;
    Ok(pool)
}

/// Apply the initial schema. Idempotent (every statement is `IF NOT EXISTS`).
pub async fn run_migrations(pool: &SqlitePool) -> Result<(), ModelError> {
    sqlx::raw_sql(include_str!("../migrations/0001_init.sql"))
        .execute(pool)
        .await?;
    Ok(())
}

/// Upsert a position (insert, or overwrite its fields if the id already
/// exists) and return its id.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_position(
    pool: &SqlitePool,
    id: i64,
    ilk: &str,
    vault: &str,
    buffer: &str,
    proxy: &str,
    venue_pool: &str,
    venue_atoken: &str,
    venue_vdebt: &str,
    inception_block: u64,
) -> Result<i64, ModelError> {
    sqlx::query(
        "INSERT INTO positions
           (id, ilk, vault, buffer, proxy, venue_pool, venue_atoken, venue_vdebt, inception_block)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           ilk = excluded.ilk, vault = excluded.vault, buffer = excluded.buffer,
           proxy = excluded.proxy, venue_pool = excluded.venue_pool,
           venue_atoken = excluded.venue_atoken, venue_vdebt = excluded.venue_vdebt,
           inception_block = excluded.inception_block",
    )
    .bind(id)
    .bind(ilk)
    .bind(vault)
    .bind(buffer)
    .bind(proxy)
    .bind(venue_pool)
    .bind(venue_atoken)
    .bind(venue_vdebt)
    .bind(inception_block as i64)
    .execute(pool)
    .await?;
    Ok(id)
}

fn debt_kind_str(k: DebtKind) -> &'static str {
    match k {
        DebtKind::Draw => "Draw",
        DebtKind::Wipe => "Wipe",
    }
}

fn supply_kind_str(k: SupplyKind) -> &'static str {
    match k {
        SupplyKind::Deposit => "Deposit",
        SupplyKind::Withdraw => "Withdraw",
    }
}

async fn insert_debt_event_exec<'a, E>(
    exec: E,
    position_id: i64,
    e: &DebtEvent,
) -> Result<(), ModelError>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT OR IGNORE INTO debt_events
           (position_id, block, ts, log_index, kind, wad)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(position_id)
    .bind(e.block as i64)
    .bind(e.ts as i64)
    .bind(e.log_index as i64)
    .bind(debt_kind_str(e.kind))
    .bind(e.wad.to_string())
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn insert_debt_event(
    pool: &SqlitePool,
    position_id: i64,
    e: &DebtEvent,
) -> Result<(), ModelError> {
    insert_debt_event_exec(pool, position_id, e).await
}

async fn insert_supply_event_exec<'a, E>(
    exec: E,
    position_id: i64,
    e: &SupplyEvent,
) -> Result<(), ModelError>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT OR IGNORE INTO supply_events
           (position_id, block, ts, log_index, kind, amount, index_ray)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(position_id)
    .bind(e.block as i64)
    .bind(e.ts as i64)
    .bind(e.log_index as i64)
    .bind(supply_kind_str(e.kind))
    .bind(e.amount.to_string())
    .bind(e.index_ray.to_string())
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn insert_supply_event(
    pool: &SqlitePool,
    position_id: i64,
    e: &SupplyEvent,
) -> Result<(), ModelError> {
    insert_supply_event_exec(pool, position_id, e).await
}

async fn insert_ssr_change_exec<'a, E>(
    exec: E,
    e: &SsrChange,
    ssr_ray: u128,
) -> Result<(), ModelError>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT OR IGNORE INTO ssr_changes
           (block, ts, log_index, ssr_ray, ssr_apy)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(e.block as i64)
    .bind(e.ts as i64)
    .bind(e.log_index as i64)
    .bind(ssr_ray.to_string())
    .bind(e.ssr_apy)
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn insert_ssr_change(
    pool: &SqlitePool,
    e: &SsrChange,
    ssr_ray: u128,
) -> Result<(), ModelError> {
    insert_ssr_change_exec(pool, e, ssr_ray).await
}

async fn insert_reserve_update_exec<'a, E>(
    exec: E,
    position_id: i64,
    e: &ReserveUpdate,
    borrow_rate_ray: u128,
    total_supplied: u128,
    total_borrowed: u128,
) -> Result<(), ModelError>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT OR IGNORE INTO reserve_updates
           (position_id, block, ts, log_index, liq_rate_ray, borrow_rate_ray,
            total_supplied, total_borrowed, util_ray)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(position_id)
    .bind(e.block as i64)
    .bind(e.ts as i64)
    .bind(e.log_index as i64)
    .bind(e.liq_rate_ray.to_string())
    .bind(borrow_rate_ray.to_string())
    .bind(total_supplied.to_string())
    .bind(total_borrowed.to_string())
    .bind(e.util_ray.to_string())
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn insert_reserve_update(
    pool: &SqlitePool,
    position_id: i64,
    e: &ReserveUpdate,
    borrow_rate_ray: u128,
    total_supplied: u128,
    total_borrowed: u128,
) -> Result<(), ModelError> {
    insert_reserve_update_exec(
        pool,
        position_id,
        e,
        borrow_rate_ray,
        total_supplied,
        total_borrowed,
    )
    .await
}

async fn advance_cursor_exec<'a, E>(exec: E, position_id: i64, block: u64) -> Result<(), ModelError>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT INTO cursor (position_id, last_block) VALUES (?, ?)
         ON CONFLICT(position_id) DO UPDATE SET last_block = excluded.last_block
         WHERE excluded.last_block > cursor.last_block",
    )
    .bind(position_id)
    .bind(block as i64)
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn advance_cursor(
    pool: &SqlitePool,
    position_id: i64,
    block: u64,
) -> Result<(), ModelError> {
    advance_cursor_exec(pool, position_id, block).await
}

pub async fn get_cursor(pool: &SqlitePool, position_id: i64) -> Result<Option<u64>, ModelError> {
    let row = sqlx::query("SELECT last_block FROM cursor WHERE position_id = ?")
        .bind(position_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| {
        let b: i64 = r.get("last_block");
        b as u64
    }))
}

/// Ingest a batch of events and advance the cursor atomically: every insert and
/// the cursor update happen inside a single transaction, so a crash mid-batch
/// leaves the store consistent (all-or-nothing).
#[allow(clippy::type_complexity)]
pub async fn ingest_batch(
    pool: &SqlitePool,
    position_id: i64,
    debts: &[DebtEvent],
    supplies: &[SupplyEvent],
    ssrs: &[(SsrChange, u128)],
    reserves: &[(ReserveUpdate, u128, u128, u128)],
    new_cursor: u64,
) -> Result<(), ModelError> {
    let mut tx = pool.begin().await?;
    for e in debts {
        insert_debt_event_exec(&mut *tx, position_id, e).await?;
    }
    for e in supplies {
        insert_supply_event_exec(&mut *tx, position_id, e).await?;
    }
    for (e, ssr_ray) in ssrs {
        insert_ssr_change_exec(&mut *tx, e, *ssr_ray).await?;
    }
    for (e, borrow_rate_ray, total_supplied, total_borrowed) in reserves {
        insert_reserve_update_exec(
            &mut *tx,
            position_id,
            e,
            *borrow_rate_ray,
            *total_supplied,
            *total_borrowed,
        )
        .await?;
    }
    advance_cursor_exec(&mut *tx, position_id, new_cursor).await?;
    tx.commit().await?;
    Ok(())
}

// --- Read side: reconstruct the domain event streams for a position. -------
//
// Every u128 column is stored as decimal TEXT (see the insert side), so reads
// parse it back with `?` — a corrupt/non-numeric cell surfaces as
// `ModelError::Parse` rather than a panic. Rows are ordered by (block,
// log_index) so downstream `accrual::fold` sees a chain-consistent sequence.

fn debt_kind_from_str(s: &str) -> DebtKind {
    // DB CHECK(kind IN ('Draw','Wipe')) guarantees one of these two.
    match s {
        "Draw" => DebtKind::Draw,
        _ => DebtKind::Wipe,
    }
}

fn supply_kind_from_str(s: &str) -> SupplyKind {
    // DB CHECK(kind IN ('Deposit','Withdraw')) guarantees one of these two.
    match s {
        "Deposit" => SupplyKind::Deposit,
        _ => SupplyKind::Withdraw,
    }
}

pub async fn load_debt_events(
    pool: &SqlitePool,
    position_id: i64,
) -> Result<Vec<DebtEvent>, ModelError> {
    let rows = sqlx::query(
        "SELECT block, ts, log_index, kind, wad FROM debt_events
         WHERE position_id = ? ORDER BY block, log_index",
    )
    .bind(position_id)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let wad: String = r.get("wad");
        out.push(DebtEvent {
            block: r.get::<i64, _>("block") as u64,
            ts: r.get::<i64, _>("ts") as u64,
            log_index: r.get::<i64, _>("log_index") as u64,
            kind: debt_kind_from_str(&r.get::<String, _>("kind")),
            wad: wad.parse()?,
        });
    }
    Ok(out)
}

pub async fn load_supply_events(
    pool: &SqlitePool,
    position_id: i64,
) -> Result<Vec<SupplyEvent>, ModelError> {
    let rows = sqlx::query(
        "SELECT block, ts, log_index, kind, amount, index_ray FROM supply_events
         WHERE position_id = ? ORDER BY block, log_index",
    )
    .bind(position_id)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let amount: String = r.get("amount");
        let index_ray: String = r.get("index_ray");
        out.push(SupplyEvent {
            block: r.get::<i64, _>("block") as u64,
            ts: r.get::<i64, _>("ts") as u64,
            log_index: r.get::<i64, _>("log_index") as u64,
            kind: supply_kind_from_str(&r.get::<String, _>("kind")),
            amount: amount.parse()?,
            index_ray: index_ray.parse()?,
        });
    }
    Ok(out)
}

/// SSR changes are position-independent (the sUSDS savings rate is global), so
/// this loads the whole `ssr_changes` table.
pub async fn load_ssr_changes(pool: &SqlitePool) -> Result<Vec<SsrChange>, ModelError> {
    let rows = sqlx::query(
        "SELECT block, ts, log_index, ssr_apy FROM ssr_changes
         ORDER BY block, log_index",
    )
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(SsrChange {
            block: r.get::<i64, _>("block") as u64,
            ts: r.get::<i64, _>("ts") as u64,
            log_index: r.get::<i64, _>("log_index") as u64,
            ssr_apy: r.get::<f64, _>("ssr_apy"),
        });
    }
    Ok(out)
}

pub async fn load_reserve_updates(
    pool: &SqlitePool,
    position_id: i64,
) -> Result<Vec<ReserveUpdate>, ModelError> {
    let rows = sqlx::query(
        "SELECT block, ts, log_index, liq_rate_ray, util_ray FROM reserve_updates
         WHERE position_id = ? ORDER BY block, log_index",
    )
    .bind(position_id)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let liq_rate_ray: String = r.get("liq_rate_ray");
        let util_ray: String = r.get("util_ray");
        out.push(ReserveUpdate {
            block: r.get::<i64, _>("block") as u64,
            ts: r.get::<i64, _>("ts") as u64,
            log_index: r.get::<i64, _>("log_index") as u64,
            liq_rate_ray: liq_rate_ray.parse()?,
            util_ray: util_ray.parse()?,
        });
    }
    Ok(out)
}
