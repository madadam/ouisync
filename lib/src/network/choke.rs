use std::sync::Arc;

use tokio::{
    sync::{Semaphore, SemaphorePermit, TryAcquireError},
    time::{self, Instant},
};

use super::constants::{MAX_UNCHOKED_COUNT, MAX_UNCHOKED_DURATION};

/// Mechanism to ensure only a given number of peers are active (sending/receiving messages from us)
/// at any given time but rotates them in regular intervals so that every peer gets equal time share.
#[derive(Clone)]
pub(super) struct Choker {
    shared: Arc<Semaphore>,
}

impl Choker {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Semaphore::new(MAX_UNCHOKED_COUNT)),
        }
    }

    pub fn choke(&self) -> Choked<'_> {
        Choked {
            shared: &self.shared,
        }
    }
}

pub(super) struct Choked<'a> {
    shared: &'a Semaphore,
}

impl<'a> Choked<'a> {
    pub async fn unchoke(self) -> Unchoked<'a> {
        // unwrap is OK because we never close the semaphore.
        let permit = self.shared.acquire().await.unwrap();
        let expiry = Instant::now() + MAX_UNCHOKED_DURATION;

        Unchoked {
            shared: self.shared,
            permit,
            expiry,
        }
    }
}

pub(super) struct Unchoked<'a> {
    shared: &'a Semaphore,
    permit: SemaphorePermit<'a>,
    expiry: Instant,
}

impl<'a> Unchoked<'a> {
    pub async fn choke(mut self) -> Choked<'a> {
        loop {
            time::sleep_until(self.expiry).await;

            drop(self.permit);

            self.permit = match self.shared.try_acquire() {
                Ok(permit) => permit,
                Err(TryAcquireError::NoPermits) => {
                    break Choked {
                        shared: self.shared,
                    }
                }
                Err(TryAcquireError::Closed) => unreachable!(),
            }
        }
    }
}
