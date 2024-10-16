use crate::{
    collections::{HashMap, HashSet},
    deadlock::BlockingMutex,
    protocol::BlockId,
};
use slab::Slab;
use std::{fmt, sync::Arc};
use tokio::sync::watch;

/// Helper for tracking required missing blocks.
#[derive(Clone)]
pub(crate) struct BlockTracker {
    shared: Arc<Shared>,
}

impl BlockTracker {
    pub fn new() -> Self {
        let (notify_tx, _) = watch::channel(());

        Self {
            shared: Arc::new(Shared {
                inner: BlockingMutex::new(Inner {
                    missing_blocks: HashMap::default(),
                    clients: Slab::new(),
                }),
                notify_tx,
            }),
        }
    }

    /// Mark the block with the given id as required.
    pub fn require(&self, block_id: BlockId) {
        let mut inner = self.shared.inner.lock().unwrap();

        let missing_block = inner
            .missing_blocks
            .entry(block_id)
            .or_insert_with(|| MissingBlock {
                clients: HashSet::default(),
                accepted_by: None,
                required: false,
                approved: false,
            });

        if missing_block.required {
            return;
        }

        tracing::trace!(?block_id, "require");

        missing_block.required = true;

        if !missing_block.clients.is_empty() {
            self.shared.notify();
        }
    }

    /// Approve the block request if offered.
    pub fn approve(&self, block_id: &BlockId) {
        let mut inner = self.shared.inner.lock().unwrap();

        let Some(missing_block) = inner.missing_blocks.get_mut(block_id) else {
            return;
        };

        if missing_block.approved {
            return;
        }

        tracing::trace!(?block_id, "approve");

        missing_block.approved = true;

        // If required and offered, notify the waiting acceptors.
        if missing_block.required && !missing_block.clients.is_empty() {
            self.shared.notify();
        }
    }

    pub fn client(&self) -> BlockTrackerClient {
        let client_id = self
            .shared
            .inner
            .lock()
            .unwrap()
            .clients
            .insert(HashSet::default());

        let notify_rx = self.shared.notify_tx.subscribe();

        BlockTrackerClient {
            shared: self.shared.clone(),
            client_id,
            notify_rx,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum OfferState {
    Pending,
    Approved,
}

pub(crate) struct BlockTrackerClient {
    shared: Arc<Shared>,
    client_id: ClientId,
    notify_rx: watch::Receiver<()>,
}

impl BlockTrackerClient {
    pub fn acceptor(&self) -> BlockPromiseAcceptor {
        BlockPromiseAcceptor {
            shared: self.shared.clone(),
            client_id: self.client_id,
            notify_rx: self.notify_rx.clone(),
        }
    }

    /// Offer to request the given block by the client with `client_id` if it is, or will become,
    /// required. Returns `true` if this block was offered for the first time (by any client), `false` if it was
    /// already offered before but not yet accepted or cancelled.
    pub fn offer(&self, block_id: BlockId, state: OfferState) -> bool {
        let mut inner = self.shared.inner.lock().unwrap();

        if !inner.clients[self.client_id].insert(block_id) {
            // Already offered
            return false;
        }

        tracing::trace!(?block_id, ?state, "offer");

        let missing_block = inner
            .missing_blocks
            .entry(block_id)
            .or_insert_with(|| MissingBlock {
                clients: HashSet::default(),
                accepted_by: None,
                required: false,
                approved: false,
            });

        missing_block.clients.insert(self.client_id);

        match state {
            OfferState::Approved => {
                missing_block.approved = true;
                self.shared.notify();
            }
            OfferState::Pending => (),
        }

        true
    }
}

impl Drop for BlockTrackerClient {
    fn drop(&mut self) {
        let mut inner = self.shared.inner.lock().unwrap();
        let block_ids = inner.clients.remove(self.client_id);
        let mut notify = false;

        for block_id in block_ids {
            // unwrap is ok because of the invariant in `Inner`
            let missing_block = inner.missing_blocks.get_mut(&block_id).unwrap();

            missing_block.clients.remove(&self.client_id);

            if missing_block.unaccept_by(self.client_id) {
                notify = true;
            }

            // TODO: if the block hasn't other offers and isn't required, remove it
        }

        if notify {
            self.shared.notify()
        }
    }
}

pub(crate) struct BlockPromiseAcceptor {
    shared: Arc<Shared>,
    client_id: ClientId,
    notify_rx: watch::Receiver<()>,
}

impl BlockPromiseAcceptor {
    /// Returns the next required and offered block request. If there is no such request at the
    /// moment this function is called, waits until one appears.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept(&mut self) -> BlockPromise {
        loop {
            if let Some(block_promise) = self.try_accept() {
                tracing::trace!(block_id = ?block_promise.block_id, "accept");
                return block_promise;
            }

            // unwrap is ok because the sender exists in self.shared.
            self.notify_rx.changed().await.unwrap();
        }
    }

    /// Returns the next required and offered block request or `None` if there is no such request
    /// currently.
    pub fn try_accept(&self) -> Option<BlockPromise> {
        let mut inner = self.shared.inner.lock().unwrap();
        let inner = &mut *inner;

        // TODO: OPTIMIZE (but profile first) this linear lookup
        for block_id in &inner.clients[self.client_id] {
            // unwrap is ok because of the invariant in `Inner`
            let missing_block = inner.missing_blocks.get_mut(block_id).unwrap();

            if missing_block.required
                && missing_block.approved
                && missing_block.accepted_by.is_none()
            {
                missing_block.accepted_by = Some(self.client_id);

                return Some(BlockPromise {
                    shared: self.shared.clone(),
                    client_id: self.client_id,
                    block_id: *block_id,
                    complete: false,
                });
            }
        }

        None
    }
}

/// Represents an accepted block request.
pub(crate) struct BlockPromise {
    shared: Arc<Shared>,
    client_id: ClientId,
    block_id: BlockId,
    complete: bool,
}

impl BlockPromise {
    pub(crate) fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    /// Mark the block request as successfully completed.
    pub fn complete(mut self) {
        let mut inner = self.shared.inner.lock().unwrap();

        let Some(missing_block) = inner.missing_blocks.remove(&self.block_id) else {
            return;
        };

        for client_id in missing_block.clients {
            if let Some(block_ids) = inner.clients.get_mut(client_id) {
                block_ids.remove(&self.block_id);
            }
        }

        tracing::trace!(block_id = ?self.block_id, "complete");

        self.complete = true;
    }
}

impl fmt::Debug for BlockPromise {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BlockPromise")
            .field("client_id", &self.client_id)
            .field("block_id", &self.block_id)
            .finish()
    }
}

impl Drop for BlockPromise {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        let mut inner = self.shared.inner.lock().unwrap();

        let client = match inner.clients.get_mut(self.client_id) {
            Some(client) => client,
            None => return,
        };

        if !client.remove(&self.block_id) {
            return;
        }

        // unwrap is ok because of the invariant in `Inner`
        let missing_block = inner.missing_blocks.get_mut(&self.block_id).unwrap();
        missing_block.clients.remove(&self.client_id);

        if missing_block.unaccept_by(self.client_id) {
            self.shared.notify();
        }
    }
}

struct Shared {
    inner: BlockingMutex<Inner>,
    notify_tx: watch::Sender<()>,
}

impl Shared {
    fn notify(&self) {
        self.notify_tx.send(()).unwrap_or(())
    }
}

// Invariant: for all `block_id` and `client_id` such that
//
//     missing_blocks[block_id].clients.contains(client_id)
//
// it must hold that
//
//     clients[client_id].contains(block_id)
//
// and vice-versa.
struct Inner {
    missing_blocks: HashMap<BlockId, MissingBlock>,
    clients: Slab<HashSet<BlockId>>,
}

#[derive(Debug)]
struct MissingBlock {
    clients: HashSet<ClientId>,
    accepted_by: Option<ClientId>,
    required: bool,
    approved: bool,
}

impl MissingBlock {
    fn unaccept_by(&mut self, client_id: ClientId) -> bool {
        if let Some(accepted_by) = &self.accepted_by {
            if accepted_by == &client_id {
                self.accepted_by = None;
                return true;
            }
        }

        false
    }
}

type ClientId = usize;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collections::HashSet, protocol::Block, test_utils};
    use futures_util::future;
    use rand::{distributions::Standard, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
    use std::{pin::pin, time::Duration};
    use test_strategy::proptest;
    use tokio::{select, sync::mpsc, sync::Barrier, task, time};

    #[test]
    fn simple() {
        let tracker = BlockTracker::new();

        let client = tracker.client();

        // Initially no blocks are returned
        assert!(client.acceptor().try_accept().is_none());

        // Offered but not required blocks are not returned
        let block0: Block = rand::random();
        client.offer(block0.id, OfferState::Approved);
        assert!(client.acceptor().try_accept().is_none());

        // Required but not offered blocks are not returned
        let block1: Block = rand::random();
        tracker.require(block1.id);
        assert!(client.acceptor().try_accept().is_none());

        // Required + offered blocks are returned...
        tracker.require(block0.id);
        assert_eq!(
            client
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block0.id)
        );

        // ...but only once.
        assert!(client.acceptor().try_accept().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simple_async() {
        let tracker = BlockTracker::new();

        let block: Block = rand::random();
        let client = tracker.client();
        let mut acceptor = client.acceptor();

        tracker.require(block.id);

        let (tx, mut rx) = mpsc::channel(1);

        let handle = tokio::task::spawn(async move {
            let mut accept_task = pin!(acceptor.accept());

            loop {
                select! {
                    block_promise = &mut accept_task => {
                        return *block_promise.block_id();
                    },
                    _ = tx.send(()) => {}
                }
            }
        });

        // Make sure acceptor started accepting.
        rx.recv().await.unwrap();

        client.offer(block.id, OfferState::Approved);

        let accepted_block_id = time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("timeout")
            .unwrap();

        assert_eq!(block.id, accepted_block_id);
    }

    #[test]
    fn fallback_on_cancel_after_accept() {
        let tracker = BlockTracker::new();

        let client0 = tracker.client();
        let client1 = tracker.client();

        let block: Block = rand::random();

        tracker.require(block.id);
        client0.offer(block.id, OfferState::Approved);
        client1.offer(block.id, OfferState::Approved);

        let block_promise = client0.acceptor().try_accept();
        assert_eq!(
            block_promise.as_ref().map(BlockPromise::block_id),
            Some(&block.id)
        );
        assert!(client1.acceptor().try_accept().is_none());

        drop(block_promise);

        assert!(client0.acceptor().try_accept().is_none());
        assert_eq!(
            client1
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block.id)
        );
    }

    #[test]
    fn fallback_on_client_drop_after_require_before_accept() {
        let tracker = BlockTracker::new();

        let client0 = tracker.client();
        let client1 = tracker.client();

        let block: Block = rand::random();

        client0.offer(block.id, OfferState::Approved);
        client1.offer(block.id, OfferState::Approved);

        tracker.require(block.id);

        drop(client0);

        assert_eq!(
            client1
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block.id)
        );
    }

    #[test]
    fn fallback_on_client_drop_after_require_after_accept() {
        let tracker = BlockTracker::new();

        let client0 = tracker.client();
        let client1 = tracker.client();

        let block: Block = rand::random();

        client0.offer(block.id, OfferState::Approved);
        client1.offer(block.id, OfferState::Approved);

        tracker.require(block.id);

        let block_promise = client0.acceptor().try_accept();

        assert_eq!(
            block_promise.as_ref().map(BlockPromise::block_id),
            Some(&block.id)
        );
        assert!(client1.acceptor().try_accept().is_none());

        drop(client0);

        assert_eq!(
            client1
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block.id)
        );
    }

    #[test]
    fn fallback_on_client_drop_before_request() {
        let tracker = BlockTracker::new();

        let client0 = tracker.client();
        let client1 = tracker.client();

        let block: Block = rand::random();

        client0.offer(block.id, OfferState::Approved);
        client1.offer(block.id, OfferState::Approved);

        drop(client0);

        tracker.require(block.id);

        assert_eq!(
            client1
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block.id)
        );
    }

    #[test]
    fn approve() {
        let tracker = BlockTracker::new();
        let client = tracker.client();

        let block: Block = rand::random();
        tracker.require(block.id);

        client.offer(block.id, OfferState::Pending);
        assert!(client.acceptor().try_accept().is_none());

        tracker.approve(&block.id);
        assert_eq!(
            client
                .acceptor()
                .try_accept()
                .as_ref()
                .map(BlockPromise::block_id),
            Some(&block.id)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn race() {
        let num_clients = 10;

        let tracker = BlockTracker::new();
        let clients: Vec<_> = (0..num_clients).map(|_| tracker.client()).collect();

        let block: Block = rand::random();

        tracker.require(block.id);

        for client in &clients {
            client.offer(block.id, OfferState::Approved);
        }

        // Make sure all clients stay alive until we are done so that any accepted requests are not
        // released prematurely.
        let barrier = Arc::new(Barrier::new(clients.len()));

        // Run the clients in parallel
        let handles = clients.into_iter().map(|client| {
            task::spawn({
                let barrier = barrier.clone();
                async move {
                    let block_promise = client.acceptor().try_accept();
                    let result = block_promise.as_ref().map(BlockPromise::block_id).cloned();
                    barrier.wait().await;
                    result
                }
            })
        });

        let block_ids = future::try_join_all(handles).await.unwrap();

        // Exactly one client gets the block id
        let mut block_ids = block_ids.into_iter().flatten();
        assert_eq!(block_ids.next(), Some(block.id));
        assert_eq!(block_ids.next(), None);
    }

    #[proptest]
    fn stress(
        #[strategy(1usize..100)] num_blocks: usize,
        #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
    ) {
        stress_case(num_blocks, rng_seed)
    }

    fn stress_case(num_blocks: usize, rng_seed: u64) {
        let mut rng = StdRng::seed_from_u64(rng_seed);

        let tracker = BlockTracker::new();
        let client = tracker.client();

        let block_ids: Vec<BlockId> = (&mut rng).sample_iter(Standard).take(num_blocks).collect();

        enum Op {
            Require,
            Offer,
        }

        let mut ops: Vec<_> = block_ids
            .iter()
            .map(|block_id| (Op::Require, *block_id))
            .chain(block_ids.iter().map(|block_id| (Op::Offer, *block_id)))
            .collect();
        ops.shuffle(&mut rng);

        for (op, block_id) in ops {
            match op {
                Op::Require => {
                    tracker.require(block_id);
                }
                Op::Offer => {
                    client.offer(block_id, OfferState::Approved);
                }
            }
        }

        let mut block_promise = HashSet::with_capacity(block_ids.len());

        while let Some(block_id) = client
            .acceptor()
            .try_accept()
            .as_ref()
            .map(BlockPromise::block_id)
        {
            block_promise.insert(*block_id);
        }

        assert_eq!(block_promise.len(), block_ids.len());

        for block_id in &block_ids {
            assert!(block_promise.contains(block_id));
        }
    }
}
