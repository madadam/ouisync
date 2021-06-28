use super::{
    message::{Request, Response},
    message_broker::ClientStream,
};
use crate::{
    crypto::{Hash, Hashable},
    error::Result,
    index::{
        self, Index, InnerNodeMap, LeafNodeSet, MissingBlocksSummary, RootNode, INNER_LAYER_COUNT,
    },
    replica_id::ReplicaId,
    version_vector::VersionVector,
};
use std::cmp::Ordering;

pub struct Client {
    index: Index,
    their_replica_id: ReplicaId,
    stream: ClientStream,
}

impl Client {
    pub fn new(index: Index, their_replica_id: ReplicaId, stream: ClientStream) -> Self {
        Self {
            index,
            their_replica_id,
            stream,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        while self.pull_snapshot().await? {}
        Ok(())
    }

    async fn pull_snapshot(&mut self) -> Result<bool> {
        // Send version vector that is a combination of the versions of our latest snapshot and
        // their latest complete snapshot that we have. This way they respond only when they have
        // something we don't.
        let mut versions = self.latest_local_versions().await?;

        if let Some(node) =
            RootNode::load_latest_complete(&self.index.pool, &self.their_replica_id).await?
        {
            versions.merge(node.versions);
        }

        self.stream
            .send(Request::RootNode(versions))
            .await
            .unwrap_or(());

        while let Some(response) = self.stream.recv().await {
            self.handle_response(response).await?;

            if self.is_complete().await? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn handle_response(&mut self, response: Response) -> Result<()> {
        match response {
            Response::RootNode { versions, hash } => self.handle_root_node(versions, hash).await,
            Response::InnerNodes {
                parent_hash,
                inner_layer,
                nodes,
            } => {
                self.handle_inner_nodes(parent_hash, inner_layer, nodes)
                    .await
            }
            Response::LeafNodes { parent_hash, nodes } => {
                self.handle_leaf_nodes(parent_hash, nodes).await
            }
        }
    }

    async fn handle_root_node(&mut self, versions: VersionVector, hash: Hash) -> Result<()> {
        let this_versions = self.latest_local_versions().await?;
        if versions
            .partial_cmp(&this_versions)
            .map(Ordering::is_le)
            .unwrap_or(false)
        {
            return Ok(());
        }

        // TODO: take missing blocks from the request.
        let (node, changed) = RootNode::create(
            &self.index.pool,
            &self.their_replica_id,
            versions,
            hash,
            MissingBlocksSummary::default(),
        )
        .await?;
        index::detect_complete_snapshots(&self.index.pool, hash, 0).await?;

        if changed {
            self.stream
                .send(Request::InnerNodes {
                    parent_hash: node.hash,
                    inner_layer: 0,
                })
                .await
                .unwrap_or(())
        }

        Ok(())
    }

    async fn handle_inner_nodes(
        &mut self,
        parent_hash: Hash,
        inner_layer: usize,
        nodes: InnerNodeMap,
    ) -> Result<()> {
        if parent_hash != nodes.hash() {
            log::warn!("inner nodes parent hash mismatch");
            return Ok(());
        }

        let changed = nodes.save(&self.index.pool, &parent_hash).await?;
        index::detect_complete_snapshots(&self.index.pool, parent_hash, inner_layer).await?;

        if inner_layer < INNER_LAYER_COUNT - 1 {
            for node in changed {
                self.stream
                    .send(Request::InnerNodes {
                        parent_hash: node.hash,
                        inner_layer: inner_layer + 1,
                    })
                    .await
                    .unwrap_or(())
            }
        } else {
            for node in changed {
                self.stream
                    .send(Request::LeafNodes {
                        parent_hash: node.hash,
                    })
                    .await
                    .unwrap_or(())
            }
        }

        Ok(())
    }

    async fn handle_leaf_nodes(&mut self, parent_hash: Hash, nodes: LeafNodeSet) -> Result<()> {
        if parent_hash != nodes.hash() {
            log::warn!("leaf nodes parent hash mismatch");
            return Ok(());
        }

        nodes.save(&self.index.pool, &parent_hash).await?;
        index::detect_complete_snapshots(&self.index.pool, parent_hash, INNER_LAYER_COUNT).await?;

        Ok(())
    }

    async fn is_complete(&self) -> Result<bool> {
        Ok(
            RootNode::load_latest(&self.index.pool, &self.their_replica_id)
                .await?
                .map(|node| node.is_complete)
                .unwrap_or(false),
        )
    }

    // Returns the versions of the latest snapshot belonging to the local replica.
    async fn latest_local_versions(&self) -> Result<VersionVector> {
        Ok(
            RootNode::load_latest(&self.index.pool, &self.index.this_replica_id)
                .await?
                .map(|node| node.versions)
                .unwrap_or_default(),
        )
    }
}
