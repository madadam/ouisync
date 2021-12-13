use crate::blob::{Blob, Buffer, Core, Cursor, OpenBlock};

use crate::{
    block::{self, BlockId, BLOCK_SIZE},
    branch::Branch,
    crypto::{AuthTag, Cryptor},
    db,
    error::{Error, Result},
    index::BranchData,
    locator::Locator,
};
use std::{convert::TryInto, io::SeekFrom, sync::Arc};
use tokio::sync::Mutex;

use super::{Nonce, NONCE_SIZE};

pub(crate) struct Operations<'a> {
    core: &'a mut Core,
    current_block: &'a mut OpenBlock,
}

impl<'a> Operations<'a> {
    pub fn new(core: &'a mut Core, current_block: &'a mut OpenBlock) -> Self {
        Self {
            core,
            current_block,
        }
    }

    /// Was this blob modified and not flushed yet?
    pub fn is_dirty(&self) -> bool {
        self.current_block.dirty || self.core.len_dirty
    }

    /// Reads data from this blob into `buffer`, advancing the internal cursor. Returns the
    /// number of bytes actually read which might be less than `buffer.len()` if the portion of the
    /// blob past the internal cursor is smaller than `buffer.len()`.
    pub async fn read(&mut self, mut buffer: &mut [u8]) -> Result<usize> {
        let mut total_len = 0;

        loop {
            let remaining = (self.core.len - self.seek_position())
                .try_into()
                .unwrap_or(usize::MAX);
            let len = buffer.len().min(remaining);
            let len = self.current_block.content.read(&mut buffer[..len]);

            buffer = &mut buffer[len..];
            total_len += len;

            if buffer.is_empty() {
                break;
            }

            let locator = self.current_block.locator.next();
            if locator.number() >= self.block_count() {
                break;
            }

            // NOTE: unlike in `write` we create a separate transaction for each iteration. This is
            // because if we created a single transaction for the whole `read` call, then a failed
            // read could rollback the changes made in a previous iteration which would then be
            // lost. This is fine because there is going to be at most one dirty block within
            // a single `read` invocation anyway.
            let mut tx = self.db_pool().begin().await?;

            let (id, content) = read_block(
                &mut tx,
                self.core.branch.data(),
                self.core.branch.cryptor(),
                &self.core.blob_key,
                &locator,
            )
            .await?;

            self.replace_current_block(&mut tx, locator, id, content)
                .await?;

            tx.commit().await?;
        }

        Ok(total_len)
    }

    /// Read all data from this blob from the current seek position until the end and return then
    /// in a `Vec`.
    pub async fn read_to_end(&mut self) -> Result<Vec<u8>> {
        let mut buffer = vec![
            0;
            (self.core.len - self.seek_position())
                .try_into()
                .unwrap_or(usize::MAX)
        ];

        let len = self.read(&mut buffer).await?;
        buffer.truncate(len);

        Ok(buffer)
    }

    /// Writes `buffer` into this blob, advancing the blob's internal cursor.
    pub async fn write(&mut self, buffer: &[u8]) -> Result<()> {
        let mut tx = self.db_pool().begin().await?;
        self.write_in_transaction(&mut tx, buffer).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Writes into the blob in db transaction.
    pub async fn write_in_transaction(
        &mut self,
        tx: &mut db::Transaction<'_>,
        mut buffer: &[u8],
    ) -> Result<()> {
        loop {
            let len = self.current_block.content.write(buffer);

            // TODO: only set the dirty flag if the content actually changed. Otherwise overwirting
            // a block with the same content it already had would result in a new block with a new
            // version being unnecessarily created.
            if len > 0 {
                self.current_block.dirty = true;
            }

            buffer = &buffer[len..];

            if self.seek_position() > self.core.len {
                self.core.len = self.seek_position();
                self.core.len_dirty = true;
            }

            if buffer.is_empty() {
                break;
            }

            let locator = self.current_block.locator.next();
            let (id, content) = if locator.number() < self.block_count() {
                read_block(
                    tx,
                    self.core.branch.data(),
                    self.core.branch.cryptor(),
                    &self.core.blob_key,
                    &locator,
                )
                .await?
            } else {
                (rand::random(), Buffer::new())
            };

            self.replace_current_block(tx, locator, id, content).await?;
        }

        Ok(())
    }

    /// Seek to an offset in the blob.
    ///
    /// It is allowed to specify offset that is outside of the range of the blob but such offset
    /// will be clamped to be within the range.
    ///
    /// Returns the new seek position from the start of the blob.
    pub async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let mut tx = self.db_pool().begin().await?;
        let offset = self.seek_in_transaction(&mut tx, pos).await?;
        tx.commit().await?;
        Ok(offset)
    }

    /// Seek to an offset in the blob in a db transaction.
    pub async fn seek_in_transaction(
        &mut self,
        tx: &mut db::Transaction<'_>,
        pos: SeekFrom,
    ) -> Result<u64> {
        let offset = match pos {
            SeekFrom::Start(n) => n.min(self.core.len),
            SeekFrom::End(n) => {
                if n >= 0 {
                    self.core.len
                } else {
                    self.core.len.saturating_sub((-n) as u64)
                }
            }
            SeekFrom::Current(n) => {
                if n >= 0 {
                    self.seek_position()
                        .saturating_add(n as u64)
                        .min(self.core.len)
                } else {
                    self.seek_position().saturating_sub((-n) as u64)
                }
            }
        };

        let actual_offset = offset + self.header_size() as u64;
        let block_number = (actual_offset / BLOCK_SIZE as u64) as u32;
        let block_offset = (actual_offset % BLOCK_SIZE as u64) as usize;

        if block_number != self.current_block.locator.number() {
            let locator = self.locator_at(block_number);

            let (id, content) = read_block(
                tx,
                self.core.branch.data(),
                self.core.branch.cryptor(),
                &self.core.blob_key,
                &locator,
            )
            .await?;
            self.replace_current_block(tx, locator, id, content).await?;
        }

        self.current_block.content.pos = block_offset;

        Ok(offset)
    }

    /// Truncate the blob to the given length.
    pub async fn truncate(&mut self, len: u64) -> Result<()> {
        let mut tx = self.db_pool().begin().await?;
        self.truncate_in_transaction(&mut tx, len).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Truncate the blob to the given length in a db transaction.
    pub async fn truncate_in_transaction(
        &mut self,
        tx: &mut db::Transaction<'_>,
        len: u64,
    ) -> Result<()> {
        // TODO: reuse the truncated blocks on subsequent writes if the content is identical

        if len == self.core.len {
            return Ok(());
        }

        if len > self.core.len {
            // TODO: consider supporting this
            return Err(Error::OperationNotSupported);
        }

        let old_block_count = self.block_count();

        self.core.len = len;
        self.core.len_dirty = true;

        let new_block_count = self.block_count();

        if self.seek_position() > self.core.len {
            self.seek_in_transaction(tx, SeekFrom::End(0)).await?;
        }

        self.remove_blocks(
            tx,
            self.core
                .head_locator
                .sequence()
                .skip(new_block_count as usize)
                .take((old_block_count - new_block_count) as usize),
        )
        .await
    }

    /// Flushes this blob in a db transaction, ensuring that all intermediately buffered contents
    /// gets written to the store.
    ///
    /// Return true if was dirty and the flush actually took place
    pub async fn flush_in_transaction(&mut self, tx: &mut db::Transaction<'_>) -> Result<bool> {
        if !self.is_dirty() {
            return Ok(false);
        }

        self.write_len(tx).await?;
        self.write_current_block(tx).await?;

        Ok(true)
    }

    /// Removes this blob.
    pub async fn remove(&mut self) -> Result<()> {
        let mut tx = self.db_pool().begin().await?;
        self.remove_blocks(
            &mut tx,
            self.core
                .head_locator
                .sequence()
                .take(self.block_count() as usize),
        )
        .await?;
        tx.commit().await?;

        let nonce: Nonce = rand::random();
        let blob_key = self.core.branch.cryptor().derive_subkey(&nonce);

        *self.current_block = OpenBlock::new_head(self.core.head_locator, &nonce);
        self.core.blob_key = blob_key;
        self.core.len = 0;
        self.core.len_dirty = true;

        Ok(())
    }

    /// Creates a shallow copy (only the index nodes are copied, not blocks) of this blob into the
    /// specified destination branch and locator.
    pub async fn fork(&mut self, dst_branch: Branch, dst_head_locator: Locator) -> Result<Blob> {
        // This should gracefuly handled in the Blob from where this function is invoked.
        assert!(
            self.core.branch.id() != dst_branch.id() || self.core.head_locator != dst_head_locator
        );

        let mut tx = self.db_pool().begin().await?;

        for (src_locator, dst_locator) in self.locators().zip(dst_head_locator.sequence()) {
            let encoded_src_locator = src_locator.encode(self.core.branch.cryptor());
            let encoded_dst_locator = dst_locator.encode(self.core.branch.cryptor());

            let block_id = self
                .core
                .branch
                .data()
                .get(&mut tx, &encoded_src_locator)
                .await?;

            dst_branch
                .data()
                .insert(&mut tx, &block_id, &encoded_dst_locator)
                .await?;
        }

        tx.commit().await?;

        let new_core = Core {
            branch: dst_branch.clone(),
            head_locator: dst_head_locator,
            blob_key: self.core.blob_key.clone(),
            len: self.core.len,
            len_dirty: self.core.len_dirty,
        };

        let current_block = OpenBlock {
            locator: dst_head_locator.nth(self.current_block.locator.number()),
            id: self.current_block.id,
            content: self.current_block.content.clone(),
            dirty: self.current_block.dirty,
        };

        Ok(Blob::new(
            Arc::new(Mutex::new(new_core)),
            dst_head_locator,
            dst_branch,
            current_block,
        ))
    }

    pub fn db_pool(&self) -> &db::Pool {
        self.core.branch.db_pool()
    }

    pub fn locators(&self) -> impl Iterator<Item = Locator> {
        self.core
            .head_locator
            .sequence()
            .take(self.block_count() as usize)
    }

    async fn replace_current_block(
        &mut self,
        tx: &mut db::Transaction<'_>,
        locator: Locator,
        id: BlockId,
        content: Buffer,
    ) -> Result<()> {
        self.write_len(tx).await?;
        self.write_current_block(tx).await?;

        let mut content = Cursor::new(content);

        if locator.number() == 0 {
            // If head block, skip over the header.
            content.pos = self.header_size();
        }

        *self.current_block = OpenBlock {
            locator,
            id,
            content,
            dirty: false,
        };

        Ok(())
    }

    // Write the current block into the store.
    async fn write_current_block(&mut self, tx: &mut db::Transaction<'_>) -> Result<()> {
        if !self.current_block.dirty {
            return Ok(());
        }

        self.current_block.id = rand::random();

        write_block(
            tx,
            self.core.branch.data(),
            self.core.branch.cryptor(),
            &self.core.blob_key,
            &self.current_block.locator,
            &self.current_block.id,
            self.current_block.content.buffer.clone(),
        )
        .await?;

        self.current_block.dirty = false;

        Ok(())
    }

    // Write the current blob length into the blob header in the head block.
    async fn write_len(&mut self, tx: &mut db::Transaction<'_>) -> Result<()> {
        if !self.core.len_dirty {
            return Ok(());
        }

        if self.current_block.locator.number() == 0 {
            let old_pos = self.current_block.content.pos;
            self.current_block.content.pos = NONCE_SIZE;
            self.current_block.content.write_u64(self.core.len);
            self.current_block.content.pos = old_pos;
            self.current_block.dirty = true;
        } else {
            let locator = self.locator_at(0);
            let (_, buffer) = read_block(
                tx,
                self.core.branch.data(),
                self.core.branch.cryptor(),
                &self.core.blob_key,
                &locator,
            )
            .await?;

            let mut cursor = Cursor::new(buffer);
            cursor.pos = NONCE_SIZE;
            cursor.write_u64(self.core.len);

            write_block(
                tx,
                self.core.branch.data(),
                self.core.branch.cryptor(),
                &self.core.blob_key,
                &locator,
                &rand::random(),
                cursor.buffer,
            )
            .await?;
        }

        self.core.len_dirty = false;

        Ok(())
    }

    async fn remove_blocks<T>(&self, tx: &mut db::Transaction<'_>, locators: T) -> Result<()>
    where
        T: IntoIterator<Item = Locator>,
    {
        for locator in locators {
            self.core
                .branch
                .data()
                .remove(tx, &locator.encode(self.core.branch.cryptor()))
                .await?;
        }

        Ok(())
    }

    // Total number of blocks in this blob including the possibly partially filled final block.
    fn block_count(&self) -> u32 {
        // https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c
        (1 + (self.core.len + self.header_size() as u64 - 1) / BLOCK_SIZE as u64)
            .try_into()
            .unwrap_or(u32::MAX)
    }

    // Returns the current seek position from the start of the blob.
    pub fn seek_position(&mut self) -> u64 {
        self.current_block.locator.number() as u64 * BLOCK_SIZE as u64
            + self.current_block.content.pos as u64
            - self.header_size() as u64
    }

    fn header_size(&self) -> usize {
        self.core.header_size()
    }

    fn locator_at(&self, number: u32) -> Locator {
        self.core.head_locator.nth(number)
    }
}

async fn read_block(
    tx: &mut db::Transaction<'_>,
    branch: &BranchData,
    repo_key: &Cryptor,
    blob_key: &Cryptor,
    locator: &Locator,
) -> Result<(BlockId, Buffer)> {
    let (id, mut buffer, auth_tag) = load_block(tx, branch, repo_key, locator).await?;

    let counter = locator.number().into();
    let aad = id.as_ref(); // "additional associated data"

    let offset = if counter == 0 { NONCE_SIZE } else { 0 };

    blob_key.decrypt(counter, aad, &mut buffer[offset..], &auth_tag)?;

    Ok((id, buffer))
}

pub(crate) async fn load_block(
    tx: &mut db::Transaction<'_>,
    branch: &BranchData,
    cryptor: &Cryptor,
    locator: &Locator,
) -> Result<(BlockId, Buffer, AuthTag)> {
    let id = branch.get(tx, &locator.encode(cryptor)).await?;
    let mut content = Buffer::new();
    let auth_tag = block::read(tx, &id, &mut content).await?;

    Ok((id, content, auth_tag))
}

async fn write_block(
    tx: &mut db::Transaction<'_>,
    branch: &BranchData,
    repo_key: &Cryptor,
    blob_key: &Cryptor,
    locator: &Locator,
    block_id: &BlockId,
    mut buffer: Buffer,
) -> Result<()> {
    let counter = locator.number().into();
    let aad = block_id.as_ref(); // "additional associated data"

    let offset = if counter == 0 { NONCE_SIZE } else { 0 };

    let auth_tag = blob_key.encrypt(counter, aad, &mut buffer[offset..])?;

    block::write(tx, block_id, &buffer, &auth_tag).await?;
    branch
        .insert(tx, block_id, &locator.encode(repo_key))
        .await?;

    Ok(())
}