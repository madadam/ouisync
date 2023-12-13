//! A hash map whose entries expire after a timeout.

use crate::collections::{hash_map, HashMap};
use std::{
    borrow::Borrow,
    hash::Hash,
    mem,
    task::{ready, Context, Poll, Waker},
};
use tokio::time::{Duration, Instant};
use tokio_util::time::delay_queue::{DelayQueue, Key};

pub struct DelayMap<K, V> {
    items: HashMap<K, Item<V>>,
    delays: DelayQueue<K>,
    state: State,
}

impl<K, V> DelayMap<K, V> {
    pub fn new() -> Self {
        Self {
            items: HashMap::default(),
            delays: DelayQueue::default(),
            state: State::Active,
        }
    }
}

impl<K, V> DelayMap<K, V>
where
    K: Eq + Hash + Clone,
{
    // This is unused right now but keeping it around so we don't have to recreate when we need
    // it.
    #[allow(unused)]
    pub fn insert(&mut self, key: K, value: V, timeout: Duration) -> Option<V> {
        let delay_key = self.delays.insert(key.clone(), timeout);
        let old = self.items.insert(key, Item { value, delay_key })?;

        self.delays.remove(&old.delay_key);

        Some(old.value)
    }

    pub fn try_insert(&mut self, key: K) -> Option<VacantEntry<K, V>> {
        match self.items.entry(key) {
            hash_map::Entry::Vacant(item_entry) => Some(VacantEntry {
                item_entry,
                delays: &mut self.delays,
            }),
            hash_map::Entry::Occupied(_) => None,
        }
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item = self.items.remove(key)?;
        self.delays.remove(&item.delay_key);

        Some(item.value)
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.items.drain().map(|(key, item)| {
            self.delays.remove(&item.delay_key);
            (key, item.value)
        })
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[allow(unused)] // TODO: remove this allow when we use this
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Pauses the timeouts. No item expires while the map is paused.
    pub fn pause(&mut self) {
        match self.state {
            State::Active => {
                self.state = State::Paused {
                    since: Instant::now(),
                    waker: None,
                }
            }
            State::Paused { .. } => (),
        }
    }

    /// Resumes the timeout if paused (does nothing if not paused). The time the map's been
    /// paused doesn't count towards the timeouts. For example: an item is scheduled for 1s,
    /// then 0.5s passes and then the map is paused. Another 0.5s passes but the item doesn't
    /// expire yet. Then the map is resumed. The item still doesn't expire because it still had
    /// 0.5s left in its timeout then the map was paused. So the item expires only after
    /// another 0.5s.
    pub fn resume(&mut self) {
        let state = mem::replace(&mut self.state, State::Active);

        let (since, waker) = match state {
            State::Active => return,
            State::Paused { since, waker } => (since, waker),
        };

        for item in self.items.values_mut() {
            let expired = self.delays.remove(&item.delay_key);
            let deadline = expired.deadline();
            let key = expired.into_inner();

            let timeout = deadline.saturating_duration_since(since);
            item.delay_key = self.delays.insert(key, timeout);
        }

        // Wake
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Poll for the next expired item. This can be wrapped in `future::poll_fn` and awaited.
    /// Returns `Poll::Ready(None)` if the map is empty.
    pub fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<Option<(K, V)>> {
        match &mut self.state {
            State::Active => (),
            State::Paused { waker, .. } => {
                waker.get_or_insert_with(|| cx.waker().clone());
                return Poll::Pending;
            }
        }

        if let Some(expired) = ready!(self.delays.poll_expired(cx)) {
            let key = expired.into_inner();
            // unwrap is OK because an entry exists in `delays` iff it exists in `items`.
            let item = self.items.remove(&key).unwrap();

            Poll::Ready(Some((key, item.value)))
        } else {
            Poll::Ready(None)
        }
    }

    #[cfg(test)]
    pub async fn expired(&mut self) -> Option<(K, V)> {
        std::future::poll_fn(|cx| self.poll_expired(cx)).await
    }
}

impl<K, V> Default for DelayMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct VacantEntry<'a, K, V> {
    item_entry: hash_map::VacantEntry<'a, K, Item<V>>,
    delays: &'a mut DelayQueue<K>,
}

impl<'a, K, V> VacantEntry<'a, K, V>
where
    K: Clone,
{
    pub fn insert(self, value: V, timeout: Duration) -> &'a V {
        let delay_key = self.delays.insert(self.item_entry.key().clone(), timeout);
        &mut self.item_entry.insert(Item { value, delay_key }).value
    }
}

struct Item<V> {
    value: V,
    delay_key: Key,
}

enum State {
    Active,
    Paused {
        since: Instant,
        waker: Option<Waker>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use tokio::time;

    #[tokio::test(start_paused = true)]
    async fn smoke() {
        let mut map = DelayMap::new();

        map.insert("foo", 1, Duration::from_millis(200));
        map.insert("bar", 2, Duration::from_millis(100));

        let start = Instant::now();
        assert_eq!(map.expired().await, Some(("bar", 2)));
        assert_eq!(start.elapsed(), Duration::from_millis(100));

        assert_eq!(map.expired().await, Some(("foo", 1)));
        assert_eq!(start.elapsed(), Duration::from_millis(200));

        assert_eq!(map.expired().await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn pause_resume() {
        let mut map = DelayMap::new();

        map.insert("foo", 1, Duration::from_millis(100));
        map.pause();

        time::sleep(Duration::from_millis(200)).await;

        // The item would've been returned here had the map not been paused.
        assert_eq!(map.expired().now_or_never(), None);

        map.resume();

        // The time the map was paused doesn't count towards the timeout.
        assert_eq!(map.expired().now_or_never(), None);

        time::sleep(Duration::from_millis(100)).await;
        assert_eq!(map.expired().now_or_never(), Some(Some(("foo", 1))));

        map.insert("bar", 2, Duration::from_millis(100));

        // Let the item expire without polling the map.
        time::sleep(Duration::from_millis(200)).await;

        map.pause();
        assert_eq!(map.expired().now_or_never(), None);

        // The item had already been expired when the map was paused so it's returned
        // immediatelly after resume.
        map.resume();
        assert_eq!(map.expired().now_or_never(), Some(Some(("bar", 2))));
    }
}
