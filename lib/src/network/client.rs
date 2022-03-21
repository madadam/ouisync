use super::{
    message::{Request, Response},
    message_broker::ClientStream,
};
use crate::{
    block::BlockNonce,
    error::{Error, Result},
    index::{Index, InnerNodeMap, LeafNodeSet, ReceiveError, Summary, UntrustedProof},
    store,
};
use std::time::Duration;
use tokio::{select, time};

const REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct Client {
    index: Index,
    stream: ClientStream,
    report: bool,
}

impl Client {
    pub fn new(index: Index, stream: ClientStream) -> Self {
        Self {
            index,
            stream,
            report: true,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut report_interval = time::interval(REPORT_INTERVAL);

        loop {
            select! {
                Some(response) = self.stream.recv() => {
                    match self.handle_response(response).await {
                        Ok(()) => {}
                        Err(
                            error @ (ReceiveError::InvalidProof | ReceiveError::ParentNodeNotFound),
                        ) => {
                            log::warn!("failed to handle response: {}", error)
                        }
                        Err(ReceiveError::Fatal(error)) => return Err(error),
                    }
                }
                _ = report_interval.tick() => self.report().await?,
                else => break,
            }
        }

        Ok(())
    }

    async fn handle_response(&mut self, response: Response) -> Result<(), ReceiveError> {
        match response {
            Response::RootNode { proof, summary } => self.handle_root_node(proof, summary).await?,
            Response::InnerNodes(nodes) => self.handle_inner_nodes(nodes).await?,
            Response::LeafNodes(nodes) => self.handle_leaf_nodes(nodes).await?,
            Response::Block { content, nonce } => self.handle_block(content, nonce).await?,
        }

        Ok(())
    }

    async fn handle_root_node(
        &mut self,
        proof: UntrustedProof,
        summary: Summary,
    ) -> Result<(), ReceiveError> {
        let hash = proof.hash;
        let updated = self.index.receive_root_node(proof, summary).await?;

        if updated {
            self.stream.send(Request::ChildNodes(hash)).await;
        }

        Ok(())
    }

    async fn handle_inner_nodes(&self, nodes: InnerNodeMap) -> Result<(), ReceiveError> {
        let updated = self.index.receive_inner_nodes(nodes).await?;

        for hash in updated {
            self.stream.send(Request::ChildNodes(hash)).await;
        }

        Ok(())
    }

    async fn handle_leaf_nodes(&mut self, nodes: LeafNodeSet) -> Result<(), ReceiveError> {
        let updated = self.index.receive_leaf_nodes(nodes).await?;

        if !updated.is_empty() {
            self.report = true;
        }

        for block_id in updated {
            // TODO: avoid multiple clients downloading the same block
            self.stream.send(Request::Block(block_id)).await;
        }

        Ok(())
    }

    async fn handle_block(&mut self, content: Box<[u8]>, nonce: BlockNonce) -> Result<()> {
        match store::write_received_block(&self.index, &content, &nonce).await {
            Ok(_) => {
                self.report = true;
                Ok(())
            }
            // Ignore `BlockNotReferenced` errors as they only mean that the block is no longer
            // needed.
            Err(Error::BlockNotReferenced) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn report(&mut self) -> Result<()> {
        if !self.report {
            return Ok(());
        }

        log::debug!(
            "client: missing blocks: {}",
            store::count_missing_blocks(&mut *self.index.pool.acquire().await?).await?
        );
        self.report = false;

        Ok(())
    }
}
