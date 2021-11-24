use super::{
    connection::ConnectionPermitHalf,
    message::Message,
    object_stream::{ObjectRead, ObjectWrite},
};
use futures_util::{stream::SelectAll, Sink, Stream, StreamExt};
use std::{
    io,
    pin::Pin,
    sync::Mutex as BlockingMutex,
    task::{Context, Poll},
};
use tokio::{
    net::tcp,
    select,
    sync::{Mutex as AsyncMutex, Notify},
};

/// Stream of `Message` backed by a `TcpStream`. Closes on first error.
pub(super) struct MessageStream {
    inner: ObjectRead<Message, tcp::OwnedReadHalf>,
    _permit: ConnectionPermitHalf,
}

impl MessageStream {
    pub fn new(stream: tcp::OwnedReadHalf, permit: ConnectionPermitHalf) -> Self {
        Self {
            inner: ObjectRead::new(stream),
            _permit: permit,
        }
    }
}

impl Stream for MessageStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(message)),
            Poll::Ready(Some(Err(_)) | None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Sink for `Message` backend by a `TcpStream`.
pub(super) struct MessageSink {
    inner: ObjectWrite<Message, tcp::OwnedWriteHalf>,
    _permit: ConnectionPermitHalf,
}

impl MessageSink {
    pub fn new(stream: tcp::OwnedWriteHalf, permit: ConnectionPermitHalf) -> Self {
        Self {
            inner: ObjectWrite::new(stream),
            _permit: permit,
        }
    }
}

impl<'a> Sink<&'a Message> for MessageSink {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: &'a Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

/// Stream that reads `Message`s from multiple underlying TCP streams.
pub(super) struct MultiReader {
    active: AsyncMutex<SelectAll<MessageStream>>,
    new: BlockingMutex<Vec<MessageStream>>,
    new_notify: Notify,
}

impl MultiReader {
    pub fn new() -> Self {
        Self {
            active: AsyncMutex::new(SelectAll::new()),
            new: BlockingMutex::new(Vec::new()),
            new_notify: Notify::new(),
        }
    }

    pub fn add(&self, stream: MessageStream) {
        self.new.lock().unwrap().push(stream);
        self.new_notify.notify_one();
    }

    pub async fn recv(&self) -> Option<Message> {
        let mut active = self.active.lock().await;

        loop {
            active.extend(self.new.lock().unwrap().drain(..));

            select! {
                item = active.next() => return item,
                _ = self.new_notify.notified() => {}
            }
        }
    }
}

// /// Sink that writes to the first available underlying TCP stream.
// pub(super) struct MultiWriter {
//     inner: Vec<ObjectWrite<Message, tcp::OwnedWriteHalf>>,
// }
