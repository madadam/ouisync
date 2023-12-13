//! Similar to tokio::sync::watch, but has no initial value. Because there is no initial value the
//! API must be sligthly different. In particular, we don't have the `borrow` function.

use futures_util::{stream, Stream};
use tokio::sync::watch as w;
pub use w::error::RecvError;

pub struct Sender<T>(w::Sender<Option<T>>);

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), w::error::SendError<T>> {
        // Unwrap OK because we know we just wrapped the value.
        self.0
            .send(Some(value))
            .map_err(|e| w::error::SendError(e.0.unwrap()))
    }

    pub fn subscribe(&self) -> Receiver<T> {
        Receiver(self.0.subscribe())
    }
}

#[derive(Clone)]
pub struct Receiver<T>(w::Receiver<Option<T>>);

impl<T: Clone> Receiver<T> {
    pub async fn changed(&mut self) -> Result<T, w::error::RecvError> {
        loop {
            self.0.changed().await?;

            // Note: the w::Ref struct returned by `borrow` does not implement `Map`, so (I
            // think) we need to clone the value wrapped in `Option`.
            match &*self.0.borrow() {
                // It's the initial value, we ignore that one.
                None => continue,
                Some(v) => return Ok(v.clone()),
            }
        }
    }

    /// Returns a `Stream` that calls `changed` repeatedly and yields the returned values.
    pub fn as_stream(&mut self) -> impl Stream<Item = T> + '_ {
        stream::unfold(self, |rx| async {
            rx.changed().await.ok().map(|value| (value, rx))
        })
    }

    pub fn is_closed(&self) -> bool {
        // `tokio::sync::watch::Receiver` doesn't expose a `is_closed`, but we can get the same
        // information by checking whether `has_changed` returns an error as it only does so
        // when the chanel has been closed.
        self.0.has_changed().is_err()
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = w::channel(None);
    (Sender(tx), Receiver(rx))
}
