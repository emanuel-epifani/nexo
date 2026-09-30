//! Submitter side of the durable engine: `EngineHandle` is the manager's
//! channel + admission + lifecycle handle; `PendingReply` completes once the
//! writer commits the command's transaction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::brokers::BrokerError;
use crate::durable::engine::{Command, Domain};
use crate::durable::types::{Admission, Stats};

/// Shared submit-path state: command channel, admission budget, watch
/// registry, writer stats and the closed flag. The manager keeps one
/// handle; the worker clones its fields at spawn.
pub struct EngineHandle<D: Domain> {
    /// Broker kind used in error messages ("Queue", "Stream").
    label: &'static str,
    tx: mpsc::Sender<Command<D>>,
    admission: Arc<Admission>,
    watches: Arc<D::Watches>,
    stats: Arc<Stats>,
    closed: Arc<AtomicBool>,
}

impl<D: Domain> EngineHandle<D> {
    pub fn new(label: &'static str, tx: mpsc::Sender<Command<D>>, max_bytes: usize) -> Self {
        Self {
            label,
            tx,
            admission: Arc::new(Admission::new(max_bytes)),
            watches: Arc::new(D::Watches::default()),
            stats: Arc::new(Stats::default()),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn sender(&self) -> mpsc::Sender<Command<D>> {
        self.tx.clone()
    }

    pub fn admission(&self) -> &Arc<Admission> {
        &self.admission
    }

    pub fn watches(&self) -> &Arc<D::Watches> {
        &self.watches
    }

    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Flip to closed; returns the previous value (idempotent check).
    pub fn mark_closed(&self) -> bool {
        self.closed.swap(true, Ordering::AcqRel)
    }

    /// Admit + enqueue. Returns once the command is queued; the reply
    /// arrives on `PendingReply` after the commit.
    pub async fn submit(
        &self,
        op: D::Request,
        bytes: usize,
    ) -> Result<PendingReply<D>, BrokerError> {
        if self.is_closed() {
            return Err(BrokerError::storage(format!(
                "{} storage is shut down",
                self.label
            )));
        }
        self.admission.acquire(bytes).await;
        let (tx, rx) = oneshot::channel();
        match self
            .tx
            .send(Command {
                op,
                bytes,
                reply: Some(tx),
            })
            .await
        {
            Ok(()) => Ok(PendingReply {
                rx,
                label: self.label,
            }),
            Err(_) => {
                self.admission.release(bytes);
                Err(BrokerError::storage(format!(
                    "{} storage unavailable",
                    self.label
                )))
            }
        }
    }
}

/// Completion handle for a submitted command.
pub struct PendingReply<D: Domain> {
    rx: oneshot::Receiver<Result<D::Reply, BrokerError>>,
    label: &'static str,
}

impl<D: Domain> PendingReply<D> {
    /// Wrap a completion produced outside the submit path (long-poll loops
    /// resolve through the same reply channel).
    pub(crate) fn wrap(
        rx: oneshot::Receiver<Result<D::Reply, BrokerError>>,
        label: &'static str,
    ) -> Self {
        Self { rx, label }
    }

    pub async fn wait(self) -> Result<D::Reply, BrokerError> {
        self.rx.await.map_err(|_| {
            BrokerError::storage(format!("{} storage unavailable", self.label))
        })?
    }
}
