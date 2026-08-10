//! Ingest — the durable-write component.
//!
//! Receives an [`EventsBatch`] from the listener and commits it via
//! [`model::ingest_batch`], which writes every row *and* advances the cursor
//! inside one transaction (all-or-nothing). Only after the commit succeeds does
//! it ack the batch's oneshot (releasing the listener) and broadcast the newly
//! durable block on `advanced` so the exporter can regenerate its output.

use log::{debug, info, warn};
use sqlx::SqlitePool;
use thiserror::Error;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};

use crate::{error::CircuitBreakerEvent, listener::EventsBatch, model};

const LOG_TARGET: &str = "Ingest";

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("model error: {0}")]
    Model(#[from] model::ModelError),
    #[error("circuit-breaker send error: {0}")]
    CbBroadcast(#[from] broadcast::error::SendError<CircuitBreakerEvent>),
    #[error("broadcast receive error: {0}")]
    Recv(#[from] broadcast::error::RecvError),
}

pub struct Ingest;

impl Ingest {
    pub async fn run(
        pool: SqlitePool,
        position_id: i64,
        mut events_rx: mpsc::Receiver<EventsBatch>,
        advanced_tx: broadcast::Sender<u64>,
        circuit_breaker_sender: broadcast::Sender<CircuitBreakerEvent>,
        mut circuit_breaker_receiver: broadcast::Receiver<CircuitBreakerEvent>,
    ) -> Result<CircuitBreakerEvent, IngestError> {
        info!(target: LOG_TARGET, "Starting");

        loop {
            debug!(target: LOG_TARGET, "Ping");

            select! {
                cb_event = circuit_breaker_receiver.recv() => {
                    warn!(target: LOG_TARGET, "Exiting due to a circuit breaker event {cb_event:?}");
                    return Ok(cb_event?);
                },

                maybe_batch = events_rx.recv() => {
                    // None: the listener dropped its sender → coordinated shutdown.
                    let batch = match maybe_batch {
                        Some(b) => b,
                        None => {
                            warn!(target: LOG_TARGET, "events channel closed, shutting down");
                            circuit_breaker_sender.send(CircuitBreakerEvent::IngestFailure)?;
                            return Ok(CircuitBreakerEvent::IngestFailure);
                        }
                    };
                    let EventsBatch {
                        from_block,
                        to_block,
                        debts,
                        supplies,
                        ssrs,
                        reserves,
                        ack,
                    } = batch;

                    info!(
                        target: LOG_TARGET,
                        "Ingesting batch for blocks {from_block}-{to_block}"
                    );

                    // Persist everything + advance the cursor to to_block+1 in ONE txn.
                    if let Err(why) = model::ingest_batch(
                        &pool,
                        position_id,
                        &debts,
                        &supplies,
                        &ssrs,
                        &reserves,
                        to_block + 1,
                    )
                    .await
                    {
                        warn!(target: LOG_TARGET, "ingest_batch failed {why:?}, exiting");
                        circuit_breaker_sender.send(CircuitBreakerEvent::IngestFailure)?;
                        return Ok(CircuitBreakerEvent::IngestFailure);
                    }

                    // Release the listener, then signal the exporter. A dropped ack
                    // AFTER a durable commit is benign (listener shutting down): treat
                    // it as a graceful circuit-break, not a fatal error, so the
                    // supervisor reboots instead of exiting.
                    if ack.send(()).is_err() {
                        warn!(target: LOG_TARGET, "ack receiver dropped after commit, shutting down");
                        circuit_breaker_sender.send(CircuitBreakerEvent::IngestFailure)?;
                        return Ok(CircuitBreakerEvent::IngestFailure);
                    }
                    // Signal the exporter with the newly-persisted cursor (to_block+1,
                    // matching model::ingest_batch). A dropped receiver (Exporter gone)
                    // is a benign shutdown: circuit-break for a reboot, never fatal.
                    if advanced_tx.send(to_block + 1).is_err() {
                        warn!(target: LOG_TARGET, "no exporter listening; shutting down for reboot");
                        circuit_breaker_sender.send(CircuitBreakerEvent::IngestFailure)?;
                        return Ok(CircuitBreakerEvent::IngestFailure);
                    }
                }
            }
        }
    }
}
