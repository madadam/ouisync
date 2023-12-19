use super::{
    crypto::Role,
    debug_payload::{DebugRequest, DebugResponse},
    peer_exchange::PexPayload,
    runtime_id::PublicRuntimeId,
};
use crate::{
    crypto::{sign::PublicKey, Hash, Hashable},
    protocol::{
        BlockContent, BlockId, BlockNonce, InnerNodes, LeafNodes, MultiBlockPresence,
        UntrustedProof,
    },
    repository::RepositoryId,
};
use serde::{Deserialize, Serialize};
use std::io::Write;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub(crate) enum Request {
    RootNode(PublicKey, DebugRequest),
    ChildNodes(Hash, ResponseDisambiguator, DebugRequest),
    Block(BlockId, DebugRequest),
    /// Send to indicate that we've received and processed the respones to all sent requests so
    /// far. The peer should choke us on receiving this.
    /// NOTE: This doesn't have a response
    /// NOTE: Interest is signalled implicitly, by sending any other message.
    Uninterested,
}

/// ResponseDisambiguator is used to uniquelly assign a response to a request.
/// What we want to avoid is that an outdated response clears out a newer pending request.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
#[serde(transparent)]
pub(crate) struct ResponseDisambiguator(MultiBlockPresence);

impl ResponseDisambiguator {
    pub fn new(multi_block_presence: MultiBlockPresence) -> Self {
        Self(multi_block_presence)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Response {
    /// Send the latest root node of this replica to another replica.
    /// NOTE: This is both a response and notification - the server sends this as a response to
    /// `Request::RootNode` but also on its own when it detects change in the repo.
    RootNode(UntrustedProof, MultiBlockPresence, DebugResponse),
    /// Send that a RootNode request failed
    RootNodeError(PublicKey, DebugResponse),
    /// Send inner nodes.
    InnerNodes(InnerNodes, ResponseDisambiguator, DebugResponse),
    /// Send leaf nodes.
    LeafNodes(LeafNodes, ResponseDisambiguator, DebugResponse),
    /// Send that a ChildNodes request failed
    ChildNodesError(Hash, ResponseDisambiguator, DebugResponse),
    /// Send a notification that a block became available on this replica.
    /// NOTE: This is always unsolicited - the server sends it on its own when it detects a newly
    /// received block.
    BlockOffer(BlockId, DebugResponse),
    /// Send a requested block.
    Block(BlockContent, BlockNonce, DebugResponse),
    /// Send that a Block request failed
    BlockError(BlockId, DebugResponse),
    /// Send to signal the peer is choked.
    /// NOTE: This is always unsolicited.
    /// NOTE: unchoking is signalled implicitly by sending any other message.
    Choked,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub(crate) enum Type {
    KeepAlive,
    Barrier,
    Content,
}

impl Type {
    fn as_u8(&self) -> u8 {
        match self {
            Type::KeepAlive => 0,
            Type::Barrier => 1,
            Type::Content => 2,
        }
    }

    fn from_u8(tag: u8) -> Option<Type> {
        match tag {
            0 => Some(Type::KeepAlive),
            1 => Some(Type::Barrier),
            2 => Some(Type::Content),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub(crate) struct Header {
    pub tag: Type,
    pub channel: MessageChannel,
}

impl Header {
    pub(crate) const SIZE: usize = 1 + // One byte for the tag.
        Hash::SIZE; // Channel

    pub(crate) fn serialize(&self) -> [u8; Self::SIZE] {
        let mut hdr = [0; Self::SIZE];
        let mut w = ArrayWriter { array: &mut hdr };

        w.write_u8(self.tag.as_u8());
        w.write_channel(&self.channel);

        hdr
    }

    pub(crate) fn deserialize(hdr: &[u8; Self::SIZE]) -> Option<Header> {
        let mut r = ArrayReader { array: &hdr[..] };

        let tag = match Type::from_u8(r.read_u8()) {
            Some(tag) => tag,
            None => return None,
        };

        Some(Header {
            tag,
            channel: r.read_channel(),
        })
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub(crate) struct Message {
    pub tag: Type,
    pub channel: MessageChannel,
    pub content: Vec<u8>,
}

impl Message {
    pub fn new_keep_alive() -> Self {
        Self {
            tag: Type::KeepAlive,
            channel: MessageChannel::default(),
            content: Vec::new(),
        }
    }

    pub fn header(&self) -> Header {
        Header {
            tag: self.tag,
            channel: self.channel,
        }
    }

    pub fn is_keep_alive(&self) -> bool {
        self.tag == Type::KeepAlive
    }
}

impl From<(Header, Vec<u8>)> for Message {
    fn from(hdr_and_content: (Header, Vec<u8>)) -> Message {
        let hdr = hdr_and_content.0;
        let content = hdr_and_content.1;

        Self {
            tag: hdr.tag,
            channel: hdr.channel,
            content,
        }
    }
}

struct ArrayReader<'a> {
    array: &'a [u8],
}

impl ArrayReader<'_> {
    // Unwraps are OK because all sizes are known at compile time.

    fn read_u8(&mut self) -> u8 {
        let n = u8::from_le_bytes(self.array[..1].try_into().unwrap());
        self.array = &self.array[1..];
        n
    }

    fn read_channel(&mut self) -> MessageChannel {
        let hash: [u8; Hash::SIZE] = self.array[..Hash::SIZE].try_into().unwrap();
        self.array = &self.array[Hash::SIZE..];
        hash.into()
    }
}

struct ArrayWriter<'a> {
    array: &'a mut [u8],
}

impl ArrayWriter<'_> {
    // Unwraps are OK because all sizes are known at compile time.

    fn write_u8(&mut self, n: u8) {
        self.array.write_all(&n.to_le_bytes()).unwrap();
    }

    fn write_channel(&mut self, channel: &MessageChannel) {
        self.array.write_all(channel.as_ref()).unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Content {
    Request(Request),
    Response(Response),
    // Peer exchange
    Pex(PexPayload),
}

#[cfg(test)]
impl From<Content> for Request {
    fn from(content: Content) -> Self {
        match content {
            Content::Request(request) => request,
            Content::Response(_) | Content::Pex(_) => {
                panic!("not a request: {:?}", content)
            }
        }
    }
}

#[cfg(test)]
impl From<Content> for Response {
    fn from(content: Content) -> Self {
        match content {
            Content::Response(response) => response,
            Content::Request(_) | Content::Pex(_) => {
                panic!("not a response: {:?}", content)
            }
        }
    }
}

define_byte_array_wrapper! {
    // TODO: consider lower size (truncate the hash) which should still be enough to be unique
    // while reducing the message size.
    #[derive(Serialize, Deserialize)]
    pub(crate) struct MessageChannel([u8; Hash::SIZE]);
}

impl MessageChannel {
    pub(super) fn new(
        repo_id: &'_ RepositoryId,
        this_runtime_id: &'_ PublicRuntimeId,
        that_runtime_id: &'_ PublicRuntimeId,
        role: Role,
    ) -> Self {
        let (id1, id2) = match role {
            Role::Initiator => (this_runtime_id, that_runtime_id),
            Role::Responder => (that_runtime_id, this_runtime_id),
        };

        Self(
            (repo_id, id1, id2, b"ouisync message channel id")
                .hash()
                .into(),
        )
    }

    #[cfg(test)]
    pub(crate) fn random() -> Self {
        Self(rand::random())
    }
}

impl Default for MessageChannel {
    fn default() -> Self {
        Self([0; Self::SIZE])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_serialization() {
        let header = Header {
            tag: Type::Content,
            channel: MessageChannel::random(),
        };

        let serialized = header.serialize();
        assert_eq!(Header::deserialize(&serialized), Some(header));
    }
}
