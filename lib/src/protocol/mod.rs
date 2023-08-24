//! Datatypes for the ouisync protocol and for interoperability between the network layer and the
//! storage later

mod block;
mod inner_node;
mod leaf_node;
mod locator;
mod proof;
mod root_node;
mod summary;
mod version_vector_op;

#[cfg(test)]
pub(crate) mod test_utils;

pub use self::block::BLOCK_SIZE;

pub(crate) use self::{
    block::{BlockData, BlockId, BlockNonce, BLOCK_RECORD_SIZE},
    inner_node::{get_bucket, InnerNode, InnerNodeMap, EMPTY_INNER_HASH, INNER_LAYER_COUNT},
    leaf_node::{LeafNode, LeafNodeModifyStatus, LeafNodeSet, EMPTY_LEAF_HASH},
    locator::Locator,
    proof::{Proof, ProofError, UntrustedProof},
    root_node::RootNode,
    summary::{MultiBlockPresence, NodeState, SingleBlockPresence, Summary},
    version_vector_op::VersionVectorOp,
};

#[cfg(test)]
pub(crate) use self::block::BLOCK_NONCE_SIZE;
