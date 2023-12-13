use futures_util::StreamExt;
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

/// Use this class to `await` until something is `Drop`ped.
///
/// Example:
///
///     let drop_awaitable = DropAwaitable::new();
///     let on_dropped = drop_awaitable.subscribe();
///
///     tokio::spawn(async move {
///       let _drop_awaitable = drop_awaitable; // Move it to this task.
///       // Do some async tasks.
///     });
///
///     on_dropped.recv().await;
///
pub struct DropAwaitable {
    tx: watch::Sender<()>,
}

impl DropAwaitable {
    pub fn new() -> Self {
        Self {
            tx: watch::channel(()).0,
        }
    }

    pub fn subscribe(&self) -> AwaitDrop {
        AwaitDrop {
            rx: WatchStream::new(self.tx.subscribe()),
        }
    }
}

impl Default for DropAwaitable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AwaitDrop {
    rx: WatchStream<()>,
}

impl Future for AwaitDrop {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while ready!(self.rx.poll_next_unpin(cx)).is_some() {}
        Poll::Ready(())
    }
}
