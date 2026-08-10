//! Cursor — the block-coordination component.
//!
//! It owns *where the pipeline is*, but not the durable cursor row: persistence
//! is [`crate::ingest`]'s job (it advances the cursor inside the same transaction
//! that stores a batch). On start Cursor seeds the pipeline with the next block to
//! process (the persisted cursor, or the position's inception block on a cold
//! start), then acts as the turn-around point of the ping-pong: each
//! `last_processed` block the listener reports is re-broadcast as the next block
//! to fetch.
//!
//! ## Channel wiring (see `main::run`)
//! Two *separate* `broadcast::<u64>` channels carry the loop so no component ever
//! receives its own message:
//! * `next_unprocessed` — Cursor sends, Listener receives.
//! * `last_processed`   — Listener sends, Cursor receives.
//!
//! (The plan sketches "one" block-coordination channel; a single shared channel
//! would echo the listener's `last_processed` straight back into its own
//! `next_unprocessed` receiver and double-drive the loop, so it is split in two.)

use std::sync::Arc;

use log::{info, warn};
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::{
    select,
    sync::broadcast,
};

use crate::{config::Config, error::CircuitBreakerEvent, model};

const LOG_TARGET: &str = "Cursor";

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("model error: {0}")]
    Model(#[from] model::ModelError),
    #[error("circuit-breaker send error: {0}")]
    CbBroadcast(#[from] broadcast::error::SendError<CircuitBreakerEvent>),
    #[error("broadcast receive error: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

pub struct Cursor;

impl Cursor {
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        _config: Arc<Config>,
        pool: SqlitePool,
        position_id: i64,
        inception_block: u64,
        next_unprocessed_tx: broadcast::Sender<u64>,
        mut last_processed_rx: broadcast::Receiver<u64>,
        circuit_breaker_sender: broadcast::Sender<CircuitBreakerEvent>,
        mut circuit_breaker_receiver: broadcast::Receiver<CircuitBreakerEvent>,
    ) -> Result<CircuitBreakerEvent, CursorError> {
        info!(target: LOG_TARGET, "Starting");

        // Seed the next block to process: resume from the persisted cursor, else
        // cold-start at the position's inception block.
        let start = match model::get_cursor(&pool, position_id).await {
            Ok(Some(last)) => {
                info!(target: LOG_TARGET, "Resuming from persisted cursor {last}");
                last
            }
            Ok(None) => {
                info!(target: LOG_TARGET, "No cursor found, starting at inception block {inception_block}");
                inception_block
            }
            Err(why) => {
                warn!(target: LOG_TARGET, "Exiting because cursor read failed: {why:?}");
                circuit_breaker_sender.send(CircuitBreakerEvent::CursorFailure)?;
                return Ok(CircuitBreakerEvent::CursorFailure);
            }
        };

        info!(target: LOG_TARGET, "Broadcasting {start} as the first block to process");
        // A dropped receiver (Listener gone) is a benign shutdown, not fatal:
        // circuit-break so the supervisor reboots the whole set.
        if next_unprocessed_tx.send(start).is_err() {
            warn!(target: LOG_TARGET, "no listener listening; shutting down for reboot");
            circuit_breaker_sender.send(CircuitBreakerEvent::CursorFailure)?;
            return Ok(CircuitBreakerEvent::CursorFailure);
        }

        loop {
            select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting due to a circuit breaker event {cb_event:?}");
                    return Ok(cb_event?);
                },
                result = last_processed_rx.recv() => match result {
                    Ok(last_processed) => {
                        info!(target: LOG_TARGET, "Advancing pipeline to block {last_processed}");
                        if next_unprocessed_tx.send(last_processed).is_err() {
                            warn!(target: LOG_TARGET, "no listener listening; shutting down for reboot");
                            circuit_breaker_sender.send(CircuitBreakerEvent::CursorFailure)?;
                            return Ok(CircuitBreakerEvent::CursorFailure);
                        }
                    }
                    // Lagged: we fell behind; loop to receive the latest value.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(target: LOG_TARGET, "last_processed lagged by {n}, continuing");
                    }
                    // Closed: all senders gone → coordinated shutdown.
                    Err(broadcast::error::RecvError::Closed) => {
                        warn!(target: LOG_TARGET, "last_processed channel closed, shutting down");
                        circuit_breaker_sender.send(CircuitBreakerEvent::CursorFailure)?;
                        return Ok(CircuitBreakerEvent::CursorFailure);
                    }
                }
            }
        }
    }
}
