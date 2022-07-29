mod cache;
mod inner;
mod open_block;
mod operations;
#[cfg(test)]
mod tests;

pub(crate) use self::{
    cache::BlobCache,
    inner::{MaybeInitShared, Shared},
};
use self::{inner::Unique, open_block::OpenBlock, operations::Operations};
use crate::{
    blob_id::BlobId, block::BlockId, branch::Branch, db, error::Error, error::Result,
    locator::Locator, sync::Mutex,
};
use std::{io::SeekFrom, mem, sync::Arc};

/// Size of the blob header in bytes.
// Using u64 instead of usize because HEADER_SIZE must be the same irrespective of whether we're on
// a 32bit or 64bit processor (if we want two such replicas to be able to sync).
pub const HEADER_SIZE: usize = mem::size_of::<u64>();

#[derive(Clone)]
pub(crate) struct Blob {
    shared: Arc<Mutex<Shared>>,
    unique: Unique,
}

impl Blob {
    /// Opens an existing blob.
    pub async fn open(
        conn: &mut db::Connection,
        branch: Branch,
        head_locator: Locator,
        shared: MaybeInitShared,
    ) -> Result<Self> {
        let mut current_block = OpenBlock::open_head(conn, &branch, head_locator).await?;

        let len = current_block.content.read_u64();
        let shared = shared.ensure_init(len).await;

        Ok(Self {
            shared,
            unique: Unique {
                branch,
                head_locator,
                current_block,
                len_dirty: false,
            },
        })
    }

    /// Creates a new blob.
    pub fn create(branch: Branch, head_locator: Locator, shared: MaybeInitShared) -> Self {
        let current_block = OpenBlock::new_head(head_locator);

        Self {
            shared: shared.assume_init(),
            unique: Unique {
                branch,
                head_locator,
                current_block,
                len_dirty: false,
            },
        }
    }

    /// Removes a blob.
    pub async fn remove(
        conn: &mut db::Connection,
        branch: &Branch,
        head_locator: Locator,
    ) -> Result<()> {
        // TODO: we only need the first 8 bytes of the block, no need to read it all.
        let mut current_block = OpenBlock::open_head(conn, branch, head_locator).await?;
        let len = current_block.content.read_u64();
        let block_count = inner::block_count(len);

        operations::remove_blocks(conn, branch, head_locator, 0..block_count).await?;

        Ok(())
    }

    pub fn branch(&self) -> &Branch {
        &self.unique.branch
    }

    /// Locator of this blob.
    pub fn locator(&self) -> &Locator {
        &self.unique.head_locator
    }

    pub async fn len(&self) -> u64 {
        self.shared.lock().await.len
    }

    /// Reads data from this blob into `buffer`, advancing the internal cursor. Returns the
    /// number of bytes actually read which might be less than `buffer.len()` if the portion of the
    /// blob past the internal cursor is smaller than `buffer.len()`.
    pub async fn read(&mut self, conn: &mut db::Connection, buffer: &mut [u8]) -> Result<usize> {
        self.lock().await.read(conn, buffer).await
    }

    /// Read all data from this blob from the current seek position until the end and return then
    /// in a `Vec`.
    pub async fn read_to_end(&mut self, conn: &mut db::Connection) -> Result<Vec<u8>> {
        self.lock().await.read_to_end(conn).await
    }

    /// Writes `buffer` into this blob, advancing the blob's internal cursor.
    pub async fn write(&mut self, conn: &mut db::Connection, buffer: &[u8]) -> Result<()> {
        self.lock().await.write(conn, buffer).await
    }

    /// Seek to an offset in the blob.
    ///
    /// It is allowed to specify offset that is outside of the range of the blob but such offset
    /// will be clamped to be within the range.
    ///
    /// Returns the new seek position from the start of the blob.
    pub async fn seek(&mut self, conn: &mut db::Connection, pos: SeekFrom) -> Result<u64> {
        self.lock().await.seek(conn, pos).await
    }

    /// Truncate the blob to the given length.
    pub async fn truncate(&mut self, conn: &mut db::Connection, len: u64) -> Result<()> {
        self.lock().await.truncate(conn, len).await
    }

    /// Flushes this blob, ensuring that all intermediately buffered contents gets written to the
    /// store.
    pub async fn flush(&mut self, conn: &mut db::Connection) -> Result<bool> {
        self.lock().await.flush(conn).await
    }

    /// Creates a shallow copy (only the index nodes are copied, not blocks) of this blob into the
    /// specified destination branch unless the blob is already in `dst_branch`. In that case
    /// returns `Error::EntryExists`.
    pub async fn try_fork(&self, conn: &mut db::Connection, dst_branch: Branch) -> Result<Self> {
        if self.unique.branch.id() == dst_branch.id() {
            return Err(Error::EntryExists);
        }

        let read_key = self.unique.branch.keys().read();
        // Take the write key from the dst branch, not the src branch, to protect us against
        // accidentally forking into remote branch (remote branches don't have write access).
        let write_keys = dst_branch.keys().write().ok_or(Error::PermissionDenied)?;

        let shared = self.shared.lock().await;

        let locators = self
            .unique
            .head_locator
            .sequence()
            .take(shared.block_count() as usize);

        for locator in locators {
            let encoded_locator = locator.encode(read_key);

            let block_id = self
                .unique
                .branch
                .data()
                .get(conn, &encoded_locator)
                .await?;

            dst_branch
                .data()
                .insert(conn, &block_id, &encoded_locator, write_keys)
                .await?;
        }

        let forked = Self {
            shared: shared.deep_clone(),
            unique: Unique {
                branch: dst_branch,
                head_locator: self.unique.head_locator,
                current_block: self.unique.current_block.clone(),
                len_dirty: self.unique.len_dirty,
            },
        };

        Ok(forked)
    }

    /// Was this blob modified and not flushed yet?
    pub fn is_dirty(&self) -> bool {
        self.unique.current_block.dirty || self.unique.len_dirty
    }

    async fn lock(&mut self) -> Operations<'_> {
        Operations {
            shared: self.shared.lock().await,
            unique: &mut self.unique,
        }
    }
}

/// Pseudo-stream that yields the block ids of the given blob in their sequential order.
pub(crate) struct BlockIds {
    branch: Branch,
    locator: Locator,
}

impl BlockIds {
    pub fn new(branch: Branch, blob_id: BlobId) -> Self {
        Self {
            branch,
            locator: Locator::head(blob_id),
        }
    }

    pub async fn next(&mut self, conn: &mut db::Connection) -> Result<Option<BlockId>> {
        let encoded = self.locator.encode(self.branch.keys().read());
        self.locator = self.locator.next();

        match self.branch.data().get(conn, &encoded).await {
            Ok(block_id) => Ok(Some(block_id)),
            Err(Error::EntryNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
