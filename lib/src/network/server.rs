use super::{
    choke::{Choked, Choker, Unchoked},
    debug_payload::{DebugRequest, DebugResponse},
    message::{Message, Request, Response},
};
use crate::{
    crypto::{sign::PublicKey, Hash},
    error::{Error, Result},
    event::{Event, Payload},
    network::constants::UNCHOKED_IDLE_TIMEOUT,
    protocol::{BlockContent, BlockId, RootNode, RootNodeFilter},
    repository::Vault,
    store,
};
use futures_util::TryStreamExt;
use tokio::{
    select,
    sync::{
        broadcast::{self, error::RecvError},
        mpsc,
    },
    time,
};
use tracing::instrument;

pub(crate) struct Server {
    inner: Inner,
    request_rx: mpsc::Receiver<Request>,
}

impl Server {
    pub fn new(
        vault: Vault,
        message_tx: mpsc::UnboundedSender<Message>,
        request_rx: mpsc::Receiver<Request>,
        choker: Choker,
    ) -> Self {
        Self {
            inner: Inner {
                vault,
                message_tx,
                choker,
            },
            request_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let Self { inner, request_rx } = self;
        inner.run(request_rx).await
    }
}

struct Inner {
    vault: Vault,
    message_tx: mpsc::UnboundedSender<Message>,
    choker: Choker,
}

impl Inner {
    async fn run(&self, request_rx: &mut mpsc::Receiver<Request>) -> Result<()> {
        let mut event_rx = self.vault.event_tx.subscribe();
        let mut first = true;

        let mut choked = self.choker.choke();
        let mut unchoked;

        loop {
            unchoked = match self.run_choked(choked, request_rx).await? {
                Some(unchoked) => unchoked,
                None => break,
            };

            if first {
                // On the first unchoke, notify the peer about all root nodes we have.
                self.handle_unknown_event().await?;
                first = false;
            }

            choked = match self
                .run_unchoked(unchoked, request_rx, &mut event_rx)
                .await?
            {
                Some(choked) => choked,
                None => break,
            }
        }

        Ok(())
    }

    async fn run_choked<'a>(
        &'a self,
        choked: Choked<'a>,
        request_rx: &mut mpsc::Receiver<Request>,
    ) -> Result<Option<Unchoked<'a>>> {
        self.send_response(Response::Choke);

        let discard_requests = async { while request_rx.recv().await.is_some() {} };

        select! {
            _ = discard_requests => Ok(None),
            unchoked = choked.unchoke() => Ok(Some(unchoked)),
        }
    }

    async fn run_unchoked<'a>(
        &'a self,
        unchoked: Unchoked<'a>,
        request_rx: &mut mpsc::Receiver<Request>,
        event_rx: &mut broadcast::Receiver<Event>,
    ) -> Result<Option<Choked<'a>>> {
        self.send_response(Response::Unchoke);

        let handle_requests = async {
            loop {
                match time::timeout(UNCHOKED_IDLE_TIMEOUT, request_rx.recv()).await {
                    Ok(Some(request)) => {
                        self.vault.monitor.traffic.requests_received.increment(1);

                        match request {
                            Request::RootNode {
                                writer_id,
                                cookie,
                                debug,
                            } => self.handle_root_node(writer_id, cookie, debug).await?,
                            Request::ChildNodes(hash, debug) => {
                                self.handle_child_nodes(hash, debug).await?
                            }
                            Request::Block(block_id, debug) => {
                                self.handle_block(block_id, debug).await?
                            }
                            Request::Idle => return Ok(Some(self.choker.choke())),
                        }
                    }
                    Ok(None) => return Ok(None),
                    Err(_) => return Ok(Some(self.choker.choke())),
                }
            }
        };

        let handle_events = async {
            loop {
                match event_rx.recv().await {
                    Ok(event) => self.handle_event(event).await?,
                    Err(RecvError::Lagged(_)) => self.handle_unknown_event().await?,
                    Err(RecvError::Closed) => break,
                }
            }

            Ok(None)
        };

        select! {
            result = handle_requests => result,
            result = handle_events => result,
            choked = unchoked.choke() => Ok(Some(choked)),
        }
    }

    async fn handle_event(&self, event: Event) -> Result<()> {
        match event.payload {
            Payload::SnapshotApproved(branch_id) => {
                self.handle_branch_changed_event(branch_id).await
            }
            Payload::BlockReceived(block_id) => {
                self.handle_block_received_event(block_id);
                Ok(())
            }
            Payload::SnapshotRejected(_) | Payload::MaintenanceCompleted => Ok(()),
        }
    }

    #[instrument(skip(self, debug), err(Debug))]
    async fn handle_root_node(
        &self,
        writer_id: PublicKey,
        cookie: u64,
        debug: DebugRequest,
    ) -> Result<()> {
        let root_node = self
            .vault
            .store()
            .acquire_read()
            .await?
            .load_latest_approved_root_node(&writer_id, RootNodeFilter::Published)
            .await;

        match root_node {
            Ok(node) => {
                tracing::trace!("root node found");

                let response = Response::RootNode {
                    proof: node.proof.into(),
                    block_presence: node.summary.block_presence,
                    cookie,
                    debug: debug.reply(),
                };

                self.send_response(response);
                Ok(())
            }
            Err(store::Error::BranchNotFound) => {
                tracing::trace!("root node not found");
                self.send_response(Response::RootNodeError {
                    writer_id,
                    cookie,
                    debug: debug.reply(),
                });
                Ok(())
            }
            Err(error) => {
                self.send_response(Response::RootNodeError {
                    writer_id,
                    cookie,
                    debug: debug.reply(),
                });
                Err(error.into())
            }
        }
    }

    #[instrument(skip(self, debug), err(Debug))]
    async fn handle_child_nodes(&self, parent_hash: Hash, debug: DebugRequest) -> Result<()> {
        let mut reader = self.vault.store().acquire_read().await?;

        // At most one of these will be non-empty.
        let inner_nodes = reader.load_inner_nodes(&parent_hash).await?;
        let leaf_nodes = reader.load_leaf_nodes(&parent_hash).await?;

        drop(reader);

        if !inner_nodes.is_empty() || !leaf_nodes.is_empty() {
            if !inner_nodes.is_empty() {
                tracing::trace!("inner nodes found");
                self.send_response(Response::InnerNodes(inner_nodes, debug.reply()));
            }

            if !leaf_nodes.is_empty() {
                tracing::trace!("leaf nodes found");
                self.send_response(Response::LeafNodes(leaf_nodes, debug.reply()));
            }
        } else {
            tracing::trace!("child nodes not found");
            self.send_response(Response::ChildNodesError(parent_hash, debug.reply()));
        }

        Ok(())
    }

    #[instrument(skip(self, debug), err(Debug))]
    async fn handle_block(&self, block_id: BlockId, debug: DebugRequest) -> Result<()> {
        let mut content = BlockContent::new();
        let result = self
            .vault
            .store()
            .acquire_read()
            .await?
            .read_block(&block_id, &mut content)
            .await;

        match result {
            Ok(nonce) => {
                tracing::trace!("block found");
                self.send_response(Response::Block(content, nonce, debug.reply()));
                Ok(())
            }
            Err(store::Error::BlockNotFound) => {
                tracing::trace!("block not found");
                self.send_response(Response::BlockError(block_id, debug.reply()));
                Ok(())
            }
            Err(error) => {
                self.send_response(Response::BlockError(block_id, debug.reply()));
                Err(error.into())
            }
        }
    }

    #[instrument(skip(self))]
    async fn handle_branch_changed_event(&self, branch_id: PublicKey) -> Result<()> {
        let root_node = match self.load_root_node(&branch_id).await {
            Ok(node) => node,
            Err(Error::Store(store::Error::BranchNotFound)) => {
                // branch was removed after the notification was fired.
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        self.send_root_node(root_node).await
    }

    fn handle_block_received_event(&self, block_id: BlockId) {
        self.send_response(Response::BlockOffer(block_id, DebugResponse::unsolicited()));
    }

    #[instrument(skip(self))]
    async fn handle_unknown_event(&self) -> Result<()> {
        let root_nodes = self.load_root_nodes().await?;
        for root_node in root_nodes {
            self.send_root_node(root_node).await?;
        }

        Ok(())
    }

    async fn send_root_node(&self, root_node: RootNode) -> Result<()> {
        if !root_node.summary.state.is_approved() {
            // send only approved snapshots
            return Ok(());
        }

        if root_node.proof.version_vector.is_empty() {
            // Do not send snapshots with empty version vectors because they have no content yet
            return Ok(());
        }

        tracing::trace!(
            branch_id = ?root_node.proof.writer_id,
            hash = ?root_node.proof.hash,
            vv = ?root_node.proof.version_vector,
            block_presence = ?root_node.summary.block_presence,
            "send_root_node",
        );

        let response = Response::RootNode {
            proof: root_node.proof.into(),
            block_presence: root_node.summary.block_presence,
            cookie: 0,
            debug: DebugResponse::unsolicited(),
        };

        self.send_response(response);

        Ok(())
    }

    async fn load_root_nodes(&self) -> Result<Vec<RootNode>> {
        // TODO: Consider finding a way to do this in a single query. Not high priority because
        // this function is not called often.

        let mut tx = self.vault.store().begin_read().await?;

        let writer_ids: Vec<_> = tx.load_writer_ids().try_collect().await?;
        let mut root_nodes = Vec::with_capacity(writer_ids.len());

        for writer_id in writer_ids {
            match tx
                .load_latest_approved_root_node(&writer_id, RootNodeFilter::Published)
                .await
            {
                Ok(node) => root_nodes.push(node),
                Err(store::Error::BranchNotFound) => {
                    // A branch exists, but has no approved root node.
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(root_nodes)
    }

    async fn load_root_node(&self, writer_id: &PublicKey) -> Result<RootNode> {
        Ok(self
            .vault
            .store()
            .acquire_read()
            .await?
            .load_latest_approved_root_node(writer_id, RootNodeFilter::Published)
            .await?)
    }

    fn send_response(&self, response: Response) {
        if self.message_tx.send(Message::Response(response)).is_ok() {
            self.vault.monitor.traffic.responses_sent.increment(1);
        }
    }
}
