// Thin binary over the `osero_indexer` library crate (see `lib.rs`). The
// crate-level `allow(dead_code)` lives on the library, where the modules are.
use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use futures::TryFutureExt;
use log::{error, info, warn};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    task::{JoinError, JoinSet},
    time::sleep,
};

use osero_indexer::{
    config::Config,
    connections::{self, ConnError, Rpc},
    cursor::{Cursor, CursorError},
    error::CircuitBreakerEvent,
    exporter::{Exporter, ExporterError},
    ingest::{Ingest, IngestError},
    listener::{EventsBatch, Listener, ListenerError},
    model::{self, ModelError},
    position::{Position, PositionError},
};
use sqlx::SqlitePool;

/// Minimum healthy runtime before the reboot backoff resets to its default.
const MINIMUM_TASK_LENGTH: Duration = Duration::from_millis(600_000); // 10 minutes
/// Starting backoff value.
const DEFAULT_BACKOFF_DURATION: Duration = Duration::from_millis(2_000); // 2 seconds
/// Maximal backoff value.
const MAX_BACKOFF_DURATION: Duration = Duration::from_millis(600_000); // 10 minutes

/// Single-position deployment: the position row is id 1.
const POSITION_ID: i64 = 1;

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
enum IndexerError {
    #[error("invalid config: {0}")]
    Config(String),
    #[error("rpc connection error")]
    Conn(#[from] ConnError),
    #[error("model error")]
    Model(#[from] ModelError),
    #[error("position config error")]
    Position(#[from] PositionError),
    #[error("cursor component failure")]
    Cursor(#[from] CursorError),
    #[error("listener component failure")]
    Listener(#[from] ListenerError),
    #[error("ingest component failure")]
    Ingest(#[from] IngestError),
    #[error("exporter component failure")]
    Exporter(#[from] ExporterError),
    #[error("task join error")]
    Join(#[from] JoinError),
}

/// Create fresh channels and spawn the four components into `tasks`.
///
/// Two separate `broadcast::<u64>` channels carry block coordination so no
/// component receives its own message: `next_unprocessed` (Cursor → Listener) and
/// `last_processed` (Listener → Cursor). A third, `advanced`, fans the durable
/// block out to the Exporter. All receivers are created *before* any task is
/// spawned so the Cursor's seed can't outrun the Listener's subscription.
async fn run(
    tasks: &mut JoinSet<Result<CircuitBreakerEvent, IndexerError>>,
    config: Arc<Config>,
    pool: SqlitePool,
    position: Arc<Position>,
    http: Rpc,
) {
    info!("osero-indexer process is starting...");

    // Circuit breaker (MPMC, shared by all components).
    let (cb_tx, _) = broadcast::channel::<CircuitBreakerEvent>(8);

    // Listener → Ingest event batches.
    let (events_tx, events_rx) = mpsc::channel::<EventsBatch>(1);

    // Block coordination + exporter fan-out.
    let (next_unprocessed_tx, _) = broadcast::channel::<u64>(64);
    let (last_processed_tx, _) = broadcast::channel::<u64>(64);
    let (advanced_tx, _) = broadcast::channel::<u64>(64);

    // Pre-create every receiver so subscriptions exist before the first send.
    let listener_next_rx = next_unprocessed_tx.subscribe();
    let cursor_last_rx = last_processed_tx.subscribe();
    let exporter_advanced_rx = advanced_tx.subscribe();
    let cb_cursor_rx = cb_tx.subscribe();
    let cb_listener_rx = cb_tx.subscribe();
    let cb_ingest_rx = cb_tx.subscribe();
    let cb_exporter_rx = cb_tx.subscribe();

    tasks.spawn(
        Cursor::run(
            Arc::clone(&config),
            pool.clone(),
            POSITION_ID,
            position.inception_block,
            next_unprocessed_tx.clone(),
            cursor_last_rx,
            cb_tx.clone(),
            cb_cursor_rx,
        )
        .map_err(IndexerError::from),
    );

    tasks.spawn(
        Listener::run(
            Arc::clone(&config),
            http.clone(),
            Arc::clone(&position),
            events_tx,
            listener_next_rx,
            last_processed_tx.clone(),
            cb_tx.clone(),
            cb_listener_rx,
        )
        .map_err(IndexerError::from),
    );

    tasks.spawn(
        Ingest::run(
            pool.clone(),
            POSITION_ID,
            events_rx,
            advanced_tx.clone(),
            cb_tx.clone(),
            cb_ingest_rx,
        )
        .map_err(IndexerError::from),
    );

    tasks.spawn(
        Exporter::run(
            Arc::clone(&config),
            http.clone(),
            pool.clone(),
            Arc::clone(&position),
            POSITION_ID,
            exporter_advanced_rx,
            cb_tx.clone(),
            cb_exporter_rx,
        )
        .map_err(IndexerError::from),
    );
}

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    let config = Arc::new(Config::parse());
    env_logger::init();

    info!("{:#?}", &config);

    // Guard: sync_step underflows `n + sync_step - 1` in the listener at 0.
    if config.sync_step == 0 {
        return Err(IndexerError::Config("sync_step must be >= 1".to_string()));
    }

    // One-time startup: position config + DB + provider.
    let position = Arc::new(Position::load(&config.position_path)?);
    info!("Loaded position {}", position.ilk);

    let pool = model::connect(&config.db_path).await?;
    model::run_migrations(&pool).await?;
    model::upsert_position(
        &pool,
        POSITION_ID,
        &position.ilk,
        &position.vault,
        &position.buffer,
        &position.proxy,
        &position.venue_pool,
        &position.venue_atoken,
        &position.venue_vdebt,
        position.inception_block,
    )
    .await?;

    let http = connections::http_provider(&config.archive_url()).await?;

    let mut tasks = JoinSet::new();
    run(
        &mut tasks,
        Arc::clone(&config),
        pool.clone(),
        Arc::clone(&position),
        http.clone(),
    )
    .await;

    // Reboot loop: graceful component exits restart the set with growing backoff;
    // a real Err is fatal.
    let mut delay = DEFAULT_BACKOFF_DURATION;
    let mut tick = Instant::now();

    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok(reason) => {
                warn!(
                    "A component exited gracefully due to: {reason:?}, remaining: {}",
                    tasks.len()
                );

                if tasks.is_empty() {
                    let tock = tick.elapsed();
                    info!("All components exited after {tock:?}.");

                    if tock >= MINIMUM_TASK_LENGTH {
                        delay = DEFAULT_BACKOFF_DURATION;
                    } else {
                        delay = min(MAX_BACKOFF_DURATION, delay + delay / 10);
                    }
                    info!("Waiting {delay:?} before rebooting.");
                    sleep(delay).await;

                    run(
                        &mut tasks,
                        Arc::clone(&config),
                        pool.clone(),
                        Arc::clone(&position),
                        http.clone(),
                    )
                    .await;
                    tick = Instant::now();
                }
            }
            Err(why) => {
                error!("A component exited with an error {why:?}. This is fatal");
                std::process::exit(1);
            }
        }
    }

    error!("Should have never gotten here!");
    std::process::exit(1);
}
