use futures_util::{stream::Fuse, Stream, StreamExt};
use pin_project_lite::pin_project;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    fmt::Debug,
    future::Future,
    hash::Hash,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::time::{self, Duration, Instant, Sleep};

type EntryId = u64;

struct Delay {
    until: Instant,
    next: Option<EntryId>,
}

pin_project! {
    /// Rate-limitting stream adapter.
    ///
    /// ```ignore
    /// in:       |a|a|a|a|a| | | | | |a|a|a|a| | | |
    /// throttle: |a| |a| |a| | | | | |a| |a| |a| | |
    /// ```
    ///
    /// Multiple occurences of the same item within the rate-limit period are reduced to a
    /// single one but distinct items are preserved:
    ///
    /// ```ignore
    /// in:        |a|b|a|b| | | | |
    /// throttle:  |a| |a|b| | | | |
    /// ```
    pub(crate) struct Throttle<S>
    where
        S: Stream,
        S::Item: Hash,
    {
        #[pin]
        inner: Fuse<S>,
        period: Duration,
        ready: BTreeMap<EntryId, S::Item>,
        delays: HashMap<S::Item, Delay>,
        #[pin]
        sleep: Option<Sleep>,
        next_id: EntryId,
    }
}

impl<S> Throttle<S>
where
    S: Stream,
    S::Item: Eq + Hash,
{
    pub fn new(inner: S, period: Duration) -> Self {
        Self {
            inner: inner.fuse(),
            period,
            ready: BTreeMap::new(),
            delays: HashMap::new(),
            sleep: None,
            next_id: 0,
        }
    }

    fn is_ready(ready: &BTreeMap<EntryId, S::Item>, item: &S::Item) -> bool {
        for v in ready.values() {
            if v == item {
                return true;
            }
        }
        false
    }
}

impl<S> Stream for Throttle<S>
where
    S: Stream,
    S::Item: Hash + Eq + Clone + Debug,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let now = Instant::now();
        let mut inner_is_finished = false;

        // Take entries from `inner` into `items` ASAP so we can timestamp them.
        loop {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let is_ready = Self::is_ready(this.ready, &item);

                    match this.delays.entry(item.clone()) {
                        hash_map::Entry::Occupied(mut entry) => {
                            if !is_ready {
                                let delay = entry.get_mut();

                                if delay.next.is_none() {
                                    let entry_id = *this.next_id;
                                    *this.next_id += 1;
                                    delay.next = Some(entry_id);
                                }
                            }
                        }
                        hash_map::Entry::Vacant(entry) => {
                            let entry_id = *this.next_id;
                            *this.next_id += 1;

                            if !is_ready {
                                this.ready.insert(entry_id, item.clone());
                            }

                            entry.insert(Delay {
                                until: now + *this.period,
                                next: None,
                            });
                        }
                    }
                }
                Poll::Ready(None) => {
                    inner_is_finished = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }

        loop {
            if let Some(first_entry) = this.ready.first_entry() {
                return Poll::Ready(Some(first_entry.remove()));
            }

            if let Some(sleep) = this.sleep.as_mut().as_pin_mut() {
                ready!(sleep.poll(cx));
                this.sleep.set(None);
            }

            let mut first: Option<(&S::Item, &mut Delay)> = None;

            for (item, delay) in this.delays.iter_mut() {
                if let Some((_, first_delay)) = &first {
                    if (delay.until, delay.next) < (first_delay.until, first_delay.next) {
                        first = Some((item, delay));
                    }
                } else {
                    first = Some((item, delay));
                }
            }

            let (first_item, first_delay) = match &mut first {
                Some(first) => (&first.0, &mut first.1),
                None => {
                    return if inner_is_finished {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
            };

            if first_delay.until <= now {
                let first_item = (*first_item).clone();

                if first_delay.next.is_some() {
                    first_delay.until = now + *this.period;
                    first_delay.next = None;
                    return Poll::Ready(Some(first_item));
                } else {
                    this.delays.remove(&first_item);
                }
            } else {
                this.sleep.set(Some(time::sleep_until(first_delay.until)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{future, stream, StreamExt};
    use std::fmt::Debug;
    use tokio::{sync::mpsc, time::Instant};

    #[tokio::test(start_paused = true)]
    async fn rate_limit_equal_items() {
        let input = [(ms(0), 0), (ms(0), 0)];
        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (0, ms(1000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;

        //--------------------------------------------------------

        let input = [(ms(0), 0), (ms(100), 0)];
        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (0, ms(1000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;

        //--------------------------------------------------------

        let input = [(ms(0), 0), (ms(0), 0), (ms(1001), 0)];
        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (0, ms(1000)), (0, ms(2000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;

        //--------------------------------------------------------

        let input = [
            (ms(0), 0),
            (ms(100), 0),
            (ms(100), 0),
            (ms(1500), 0),
            (ms(0), 0),
        ];

        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (0, ms(1000)), (0, ms(2000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limit_inequal_items() {
        let input = [(ms(0), 0), (ms(0), 1)];

        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (1, ms(0))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;

        //--------------------------------------------------------

        let input = [(ms(0), 0), (ms(0), 1), (ms(0), 0), (ms(0), 1)];

        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (1, ms(0)), (0, ms(1000)), (1, ms(1000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;

        //--------------------------------------------------------

        let input = [(ms(0), 0), (ms(0), 1), (ms(0), 1), (ms(0), 0)];

        let (tx, rx) = mpsc::channel(1);
        let expected = [(0, ms(0)), (1, ms(0)), (1, ms(1000)), (0, ms(1000))];
        future::join(
            produce(tx, input),
            verify(Throttle::new(into_stream(rx), ms(1000)), expected),
        )
        .await;
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    fn into_stream<T>(rx: mpsc::Receiver<T>) -> impl Stream<Item = T> {
        stream::unfold(rx, |mut rx| async move { Some((rx.recv().await?, rx)) })
    }

    async fn produce<T>(tx: mpsc::Sender<T>, input: impl IntoIterator<Item = (Duration, T)>) {
        for (delay, item) in input {
            time::sleep(delay).await;
            tx.send(item).await.ok().unwrap();
        }
    }

    async fn verify<T: Eq + Debug>(
        stream: impl Stream<Item = T>,
        expected: impl IntoIterator<Item = (T, Duration)>,
    ) {
        let start = Instant::now();
        let actual: Vec<_> = stream.map(|item| (item, start.elapsed())).collect().await;
        let expected: Vec<_> = expected.into_iter().collect();

        assert_eq!(actual, expected);
    }
}
