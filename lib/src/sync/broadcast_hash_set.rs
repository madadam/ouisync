//! Similar to `tokio::sync::broadcast` but does not enqueue the values. Instead, it "accumulates"
//! them into a set. Advantages include that one doesn't need to specify the recv buffer size and
//! the `insert` (analogue to `broadcast::send`) is non-async and never fails.

use super::uninitialized_watch;
pub use super::uninitialized_watch::RecvError;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone)]
pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Eq + Hash + Clone> Sender<T> {
    pub fn insert(&self, value: &T) {
        let mut receivers = self.shared.receivers.lock().unwrap();

        for (_id, receiver) in receivers.iter_mut() {
            if receiver.1.insert(value.clone()) {
                receiver.0.send(()).unwrap_or(());
            }
        }
    }

    pub fn subscribe(&self) -> Receiver<T> {
        let id = self.shared.id_generator.fetch_add(1, Ordering::Relaxed);
        let (watch_tx, watch_rx) = uninitialized_watch::channel();

        let mut receivers = self.shared.receivers.lock().unwrap();

        receivers.insert(id, (watch_tx, HashSet::new()));

        Receiver {
            id,
            shared: self.shared.clone(),
            watch_rx,
        }
    }
}

pub(crate) struct Receiver<T> {
    id: usize,
    shared: Arc<Shared<T>>,
    watch_rx: uninitialized_watch::Receiver<()>,
}

impl<T> Receiver<T> {
    pub async fn changed(&mut self) -> Result<HashSet<T>, RecvError> {
        self.watch_rx.changed().await?;

        let mut receivers = self.shared.receivers.lock().unwrap();

        // Unwrap is OK because the entry must exists for as long as this `Receiver` exists.
        let old_set = &mut receivers.get_mut(&self.id).unwrap().1;
        let mut new_set = HashSet::new();

        if !old_set.is_empty() {
            std::mem::swap(&mut new_set, old_set);
        }

        Ok(new_set)
    }

    pub fn is_closed(&self) -> bool {
        self.watch_rx.is_closed()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.lock().unwrap().remove(&self.id);
    }
}

type ReceiverState<T> = (uninitialized_watch::Sender<()>, HashSet<T>);

struct Shared<T> {
    id_generator: AtomicUsize,
    receivers: Mutex<HashMap<usize, ReceiverState<T>>>,
}

pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let id_generator = AtomicUsize::new(0);
    let mut receivers = HashMap::new();

    let id = id_generator.fetch_add(1, Ordering::Relaxed);
    let (watch_tx, watch_rx) = uninitialized_watch::channel();

    receivers.insert(id, (watch_tx, HashSet::new()));

    let shared = Arc::new(Shared {
        id_generator,
        receivers: Mutex::new(receivers),
    });

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver {
            id,
            shared,
            watch_rx,
        },
    )
}
