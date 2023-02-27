use crate::{
    directory::Directory,
    error::{ErrorCode, Result},
    network::NetworkEvent,
    registry::Handle,
};
use ouisync_lib::{PeerInfo, Progress, StateMonitor};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{fmt, net::SocketAddr};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    Success(Response),
    Failure { code: ErrorCode, message: String },
    Notification(Notification),
}

impl ServerMessage {
    pub fn response(result: Result<Response>) -> Self {
        match result {
            Ok(response) => Self::Success(response),
            Err(error) => Self::Failure {
                code: error.to_error_code(),
                message: error.to_string(),
            },
        }
    }

    pub fn notification(notification: Notification) -> Self {
        Self::Notification(notification)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    None,
    Bool(bool),
    U8(u8),
    U32(u32),
    U64(u64),
    Bytes(ByteBuf),
    String(String),
    Handle(u64),
    Directory(Directory),
    StateMonitor(StateMonitor),
    Progress(Progress),
    PeerInfo(Vec<PeerInfo>),
}

impl<T> From<Option<T>> for Response
where
    Response: From<T>,
{
    fn from(value: Option<T>) -> Self {
        if let Some(value) = value {
            Self::from(value)
        } else {
            Self::None
        }
    }
}

impl From<()> for Response {
    fn from(_: ()) -> Self {
        Self::None
    }
}

impl From<bool> for Response {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u8> for Response {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<u32> for Response {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<u64> for Response {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<Vec<u8>> for Response {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(ByteBuf::from(value))
    }
}

impl From<String> for Response {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<StateMonitor> for Response {
    fn from(value: StateMonitor) -> Self {
        Self::StateMonitor(value)
    }
}

impl From<Directory> for Response {
    fn from(value: Directory) -> Self {
        Self::Directory(value)
    }
}

impl<T> From<Handle<T>> for Response {
    fn from(value: Handle<T>) -> Self {
        Self::Handle(value.id())
    }
}

impl From<SocketAddr> for Response {
    fn from(value: SocketAddr) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Progress> for Response {
    fn from(value: Progress) -> Self {
        Self::Progress(value)
    }
}

impl From<Vec<PeerInfo>> for Response {
    fn from(value: Vec<PeerInfo>) -> Self {
        Self::PeerInfo(value)
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Bool(value) => f.debug_tuple("Bool").field(value).finish(),
            Self::U8(value) => f.debug_tuple("U8").field(value).finish(),
            Self::U32(value) => f.debug_tuple("U32").field(value).finish(),
            Self::U64(value) => f.debug_tuple("U64").field(value).finish(),
            Self::Bytes(_) => write!(f, "Bytes(_)"),
            Self::String(value) => f.debug_tuple("String").field(value).finish(),
            Self::Handle(value) => f.debug_tuple("Handle").field(value).finish(),
            Self::Directory(_) => write!(f, "Directory(_)"),
            Self::StateMonitor(_) => write!(f, "StateMonitor(_)"),
            Self::Progress(value) => f.debug_tuple("Progress").field(value).finish(),
            Self::PeerInfo(_) => write!(f, "PeerInfo(_)"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum Notification {
    Repository,
    Network(NetworkEvent),
    StateMonitor,
}
