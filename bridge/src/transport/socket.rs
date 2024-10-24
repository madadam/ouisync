//! Low-level Client and Server tha wraps Stream/Sink of bytes. Used to implement some higher-level
//! clients/servers

use super::{Handler, TransportError};
use crate::protocol::ServerMessage;
use bytes::{Bytes, BytesMut};
use futures_util::{stream::FuturesUnordered, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, io, marker::PhantomData};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task,
};

pub mod server_connection {
    use super::*;

    pub async fn run<S, H>(mut socket: S, handler: H)
    where
        S: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin,
        H: Handler,
    {
        let (notification_tx, mut notification_rx) = mpsc::channel(1);
        let mut request_handlers = FuturesUnordered::new();

        loop {
            select! {
                message = receive(&mut socket) => {
                    let Some((id, result)) = message else {
                        break;
                    };

                    let handler = &handler;
                    let notification_tx = &notification_tx;

                    let task = async move {
                        let result = match result {
                            Ok(request) => handler.handle(request, notification_tx).await,
                            Err(error) => Err(error.into()),
                        };

                        (id, result)
                    };

                    request_handlers.push(task);
                }
                notification = notification_rx.recv() => {
                    // unwrap is OK because the sender exists at this point.
                    let (id, notification) = notification.unwrap();
                    let message = ServerMessage::<H::Response, H::Error>::notification(notification);
                    send(&mut socket, id, message).await;
                }
                Some((id, result)) = request_handlers.next() => {
                    let message = ServerMessage::response(result);
                    send(&mut socket, id, message).await;
                }
            }
        }
    }
}

pub struct SocketClient<Socket, Request, Response, Error> {
    request_tx: mpsc::Sender<(Request, oneshot::Sender<Result<Response, Error>>)>,
    _socket: PhantomData<Socket>,
}

impl<Socket, Request, Response, Error> SocketClient<Socket, Request, Response, Error>
where
    Socket: Stream<Item = io::Result<BytesMut>>
        + Sink<Bytes, Error = io::Error>
        + Unpin
        + Send
        + 'static,
    Request: Serialize + Send + 'static,
    Response: DeserializeOwned + Send + 'static,
    Error: From<TransportError> + DeserializeOwned + Send + 'static,
{
    pub fn new(socket: Socket) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1);

        task::spawn(Worker::new(request_rx, socket).run());

        Self {
            request_tx,
            _socket: PhantomData,
        }
    }

    pub async fn invoke(&self, request: Request) -> Result<Response, Error> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send((request, response_tx))
            .await
            .map_err(|_| TransportError::ConnectionLost)?;

        match response_rx
            .await
            .map_err(|_| TransportError::ConnectionLost)
        {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        }
    }
}

struct Worker<Socket, Request, Response, Error> {
    running: bool,
    request_rx: mpsc::Receiver<(Request, oneshot::Sender<Result<Response, Error>>)>,
    socket: Socket,
    pending_requests: HashMap<u64, oneshot::Sender<Result<Response, Error>>>,
    next_message_id: u64,
}

impl<Socket, Request, Response, Error> Worker<Socket, Request, Response, Error>
where
    Socket: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin + Send,
    Request: Serialize,
    Response: DeserializeOwned,
    Error: From<TransportError> + DeserializeOwned,
{
    fn new(
        request_rx: mpsc::Receiver<(Request, oneshot::Sender<Result<Response, Error>>)>,
        socket: Socket,
    ) -> Self {
        Self {
            running: true,
            request_rx,
            socket,
            pending_requests: HashMap::new(),
            next_message_id: 0,
        }
    }

    async fn run(mut self) {
        while self.running {
            select! {
                request = self.request_rx.recv() => self.handle_request(request).await,
                response = receive(&mut self.socket) => self.handle_server_message(response).await,
            }
        }

        match self.socket.close().await {
            Ok(()) => tracing::debug!("client closed"),
            Err(error) => tracing::error!(?error, "failed to close client"),
        }
    }

    async fn handle_request(
        &mut self,
        request: Option<(Request, oneshot::Sender<Result<Response, Error>>)>,
    ) {
        let Some((request, response_tx)) = request else {
            self.running = false;
            return;
        };

        let message_id = self.next_message_id;
        self.next_message_id = self.next_message_id.wrapping_add(1);
        self.pending_requests.insert(message_id, response_tx);

        if !send(&mut self.socket, message_id, request).await {
            self.running = false;
        }
    }

    async fn handle_server_message(
        &mut self,
        message: Option<(u64, ServerMessageResult<Response, Error>)>,
    ) {
        let Some((message_id, message)) = message else {
            self.running = false;
            return;
        };

        let Some(response_tx) = self.pending_requests.remove(&message_id) else {
            tracing::debug!("unsolicited response");
            return;
        };

        let response = match message {
            Ok(ServerMessage::Success(response)) => Ok(response),
            Ok(ServerMessage::Failure(error)) => Err(error),
            Ok(ServerMessage::Notification(_)) => {
                tracing::error!("notifications are not supported yet");
                return;
            }
            Err(error) => Err(error.into()),
        };

        response_tx.send(response).ok();
    }
}

async fn receive<R, T>(reader: &mut R) -> Option<(u64, Result<T, TransportError>)>
where
    R: Stream<Item = io::Result<BytesMut>> + Unpin,
    T: DeserializeOwned,
{
    loop {
        let buffer = match reader.try_next().await {
            Ok(Some(buffer)) => buffer,
            Ok(None) => return None,
            Err(error) => {
                tracing::error!(?error, "failed to receive message");
                return None;
            }
        };

        // The message id is encoded separately (big endian u64) followed by the message body
        // (messagepack encoded byte string). This allows us to decode the id even if the rest of
        // the message is malformed so that we can send error response back.

        let Some(id) = buffer.get(..8) else {
            tracing::error!("failed to decode message id");
            continue;
        };
        let id = u64::from_be_bytes(id.try_into().unwrap());

        let body = rmp_serde::from_slice(&buffer[8..]).map_err(|error| {
            tracing::error!(?error, "failed to decode message body");
            TransportError::MalformedMessage
        });

        return Some((id, body));
    }
}

async fn send<W, M>(writer: &mut W, id: u64, message: M) -> bool
where
    W: Sink<Bytes, Error = io::Error> + Unpin,
    M: Serialize,
{
    // Here we encode the id separately only for consistency with `receive`.

    let mut buffer = Vec::new();
    buffer.extend(id.to_be_bytes());

    if let Err(error) = rmp_serde::encode::write(&mut buffer, &message) {
        tracing::error!(?error, "failed to encode message");
        return false;
    };

    if let Err(error) = writer.send(buffer.into()).await {
        tracing::error!(?error, "failed to send message");
        return false;
    }

    true
}

type ServerMessageResult<R, E> = Result<ServerMessage<R, E>, TransportError>;
