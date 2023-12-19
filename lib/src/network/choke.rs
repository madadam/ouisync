use crate::collections::{HashMap, HashSet};
use rand::seq::IteratorRandom;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::Notify,
    time::{self, Duration, Instant},
};

const MAX_UNCHOKED_COUNT: usize = 3;
const MAX_UNCHOKED_DURATION: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct Manager {
    inner: Arc<Mutex<ManagerInner>>,
}

impl Manager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ManagerInner {
                next_choker_id: 0,
                choked: Default::default(),
                unchoked: Default::default(),
                notify: Arc::new(Notify::new()),
            })),
        }
    }

    /// Creates a new choker which is initially choked.
    pub fn new_choker(&self) -> Choker {
        let mut inner = self.inner.lock().unwrap();

        let id = inner.next_choker_id;
        inner.next_choker_id += 1;

        inner.choked.insert(id);

        Choker {
            manager_inner: self.inner.clone(),
            notify: inner.notify.clone(),
            id,
            choked: true,
        }
    }
}

#[derive(Clone, Copy)]
struct UnchokedState {
    unchoke_started: Instant,
}

impl UnchokedState {
    fn is_evictable(&self) -> bool {
        Instant::now() >= self.evictable_at()
    }

    fn evictable_at(&self) -> Instant {
        self.unchoke_started + MAX_UNCHOKED_DURATION
    }
}

impl Default for UnchokedState {
    fn default() -> Self {
        Self {
            unchoke_started: Instant::now(),
        }
    }
}

struct ManagerInner {
    next_choker_id: usize,
    choked: HashSet<usize>,
    unchoked: HashMap<usize, UnchokedState>,
    notify: Arc<Notify>,
}

impl ManagerInner {
    /// Tries to unchoke the given choker.
    ///
    /// # Panics
    ///
    /// Panics if `choker_id` doesn't correspond to any existing choker.
    fn try_unchoke(&mut self, choker_id: usize) -> Result<(), Instant> {
        if self.unchoked.contains_key(&choker_id) {
            return Ok(());
        }

        // Keep unchoking until we unchoke  `choker_id` or until we run out of unchoke slots.
        loop {
            let to_choke = if let Some((id, state)) = self.soonest_evictable() {
                // All unchoke slots are occupied...
                if state.is_evictable() {
                    // ...but some can be evicted.
                    Some(id)
                } else {
                    // ...and none can be evicted.
                    return Err(state.evictable_at());
                }
            } else {
                // Some unchoke slots are available.
                None
            };

            // Unwrap OK because we know `choked` is not empty (`choker_id` is in it).
            let to_unchoke = self.random_choked().unwrap();

            // Remove `to_choke` only after picking `to_unchoke`, otherwise it could be immediately
            // unchoked again.
            if let Some(to_choke) = to_choke {
                self.unchoked.remove(&to_choke);
                self.choked.insert(to_choke);
            }

            self.choked.remove(&to_unchoke);
            self.unchoked.insert(to_unchoke, UnchokedState::default());

            if to_unchoke == choker_id {
                return Ok(());
            }
        }
    }

    fn soonest_evictable(&self) -> Option<(usize, UnchokedState)> {
        if self.unchoked.len() < MAX_UNCHOKED_COUNT {
            None
        } else {
            self.unchoked
                .iter()
                .min_by_key(|(_, state)| state.evictable_at())
                .map(|(id, state)| (*id, *state))
        }
    }

    fn random_choked(&self) -> Option<usize> {
        self.choked.iter().choose(&mut rand::thread_rng()).copied()
    }

    fn remove_choker(&mut self, choker_id: usize) {
        self.choked.remove(&choker_id);
        self.unchoked.remove(&choker_id);
        self.notify.notify_waiters();
    }
}

pub(crate) struct Choker {
    manager_inner: Arc<Mutex<ManagerInner>>,
    notify: Arc<Notify>,
    id: usize,
    choked: bool,
}

impl Choker {
    /// Waits until the state changes from choked to unchoked or from unchoked to choked. Returns
    /// the new state (true = choked, false = unchoked).
    ///
    /// NOTE: The initial state of a newly created choker (before `changed` is called at least
    /// once) is always choked.
    pub async fn changed(&mut self) -> bool {
        loop {
            match (self.choked, self.try_unchoke()) {
                (true, Ok(())) => {
                    self.choked = false;
                    break;
                }
                (true, Err(sleep_until)) => {
                    time::timeout_at(sleep_until, self.notify.notified())
                        .await
                        .ok();
                }
                (false, Ok(())) => {
                    self.notify.notified().await;
                }
                (false, Err(_)) => {
                    self.choked = true;
                    break;
                }
            }
        }

        self.choked
    }

    fn try_unchoke(&self) -> Result<(), Instant> {
        self.manager_inner.lock().unwrap().try_unchoke(self.id)
    }
}

impl Drop for Choker {
    fn drop(&mut self) {
        self.manager_inner.lock().unwrap().remove_choker(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::iter;

    // use simulated time (`start_paused`) to avoid having to wait for the timeout in the real time.
    #[tokio::test(start_paused = true)]
    async fn sanity() {
        let manager = Manager::new();

        let mut chokers: Vec<_> = iter::repeat_with(|| manager.new_choker())
            .take(MAX_UNCHOKED_COUNT + 1)
            .collect();

        // All but one get unchoked immediately.
        let mut choked_index = None;

        for (index, choker) in chokers.iter_mut().enumerate() {
            if let Some(choked) = choker.changed().now_or_never() {
                assert!(!choked);
            } else {
                assert!(choked_index.is_none());
                choked_index = Some(index);
            }
        }

        let choked_index = choked_index.unwrap();

        // The choked one gets unchoked after the timeout...
        assert!(!chokers[choked_index].changed().await);

        // ...and another one gets choked instead.
        let mut choked_index = None;

        // To find the newly choked we need to be repeat this up to `MAX_UNCHOKED_COUNT` times
        // because calling `changed` on a recently choked might immediately unchoke it if there is
        // still available unchoke slot.
        for _ in 0..MAX_UNCHOKED_COUNT {
            for (index, choker) in chokers.iter_mut().enumerate() {
                if let Some(choked) = choker.changed().now_or_never() {
                    assert!(choked);
                    assert!(choked_index.is_none());
                    choked_index = Some(index);
                }
            }

            if choked_index.is_some() {
                break;
            }
        }

        let choked_index = choked_index.unwrap();

        // Remove one of the unchoked. That immediately allows to unchoke the
        // choked one.
        let remove_index = (choked_index + 1) % chokers.len();
        chokers.remove(remove_index);

        let choked_index = choked_index - if remove_index < choked_index { 1 } else { 0 };

        assert_eq!(chokers[choked_index].changed().now_or_never(), Some(false));
    }
}
