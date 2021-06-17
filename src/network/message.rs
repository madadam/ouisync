use crate::{
    crypto::Hash,
    index::{InnerNodeMap, LeafNodeSet},
    version_vector::VersionVector,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Request a root node that is newer or concurrent to the specified version.
    RootNode(VersionVector),
    /// Request inner nodes with the given parent hash and inner layer.
    InnerNodes {
        parent_hash: Hash,
        inner_layer: usize,
    },
    /// Request leaf nodes with the given parent hash.
    LeafNodes { parent_hash: Hash },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Send the latest root node of this replica to another replica.
    RootNode { versions: VersionVector, hash: Hash },
    /// Send inner nodes with the given parent hash and inner layer.
    InnerNodes {
        parent_hash: Hash,
        inner_layer: usize,
        nodes: InnerNodeMap,
    },
    /// Send leaf nodes with the given parent hash.
    LeafNodes {
        parent_hash: Hash,
        nodes: LeafNodeSet,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
}

impl From<Message> for Request {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Request(rq) => rq,
            Message::Response(_) => panic!("Message is not Request"),
        }
    }
}

impl From<Message> for Response {
    fn from(msg: Message) -> Self {
        match msg {
            Message::Request(_) => panic!("Message is not Response"),
            Message::Response(rs) => rs,
        }
    }
}
