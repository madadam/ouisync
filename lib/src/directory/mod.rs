mod content;
mod entry;
mod entry_data;
mod entry_type;
mod parent_context;
#[cfg(test)]
mod tests;

pub use self::{
    content::VERSION as DIRECTORY_VERSION,
    entry::{DirectoryRef, EntryRef, FileRef},
    entry_type::EntryType,
};
pub(crate) use self::{
    entry_data::{EntryData, EntryTombstoneData, TombstoneCause},
    parent_context::ParentContext,
};

use self::content::Content;
use crate::{
    blob::{Blob, BlobId, lock::ReadLock},
    branch::Branch,
    crypto::sign::PublicKey,
    debug::DebugPrinter,
    error::{Error, Result},
    file::File,
    protocol::{Locator, RootNode, RootNodeFilter},
    store::{self, Changeset, ReadTransaction, WriteTransaction},
    version_vector::VersionVector,
};
use async_recursion::async_recursion;
use std::{cmp::Ordering, fmt, mem};
use tracing::instrument;

#[derive(Clone)]
pub struct Directory {
    blob: Blob,
    parent: Option<ParentContext>,
    content: Content,
    lock: Option<ReadLock>,
}

#[allow(clippy::len_without_is_empty)]
impl Directory {
    /// Opens the root directory.
    pub(crate) async fn open_root(
        branch: Branch,
        locking: DirectoryLocking,
        fallback: DirectoryFallback,
    ) -> Result<Self> {
        Self::open(branch, BlobId::ROOT, None, locking, fallback).await
    }

    /// Opens the root directory or creates it if it doesn't exists.
    #[instrument(skip(branch), fields(branch_id = ?branch.id()))]
    pub(crate) async fn open_or_create_root(branch: Branch, initial_version: u64) -> Result<Self> {
        let blob_id = BlobId::ROOT;

        let lock = branch.locker().read(blob_id).await;
        let mut tx = branch.store().begin_write().await?;
        let mut changeset = Changeset::new();

        let dir = match Self::open_in(
            Some(lock.clone()),
            &mut tx,
            branch.clone(),
            blob_id,
            None,
            DirectoryFallback::Disabled,
        )
        .await
        {
            Ok(dir) => Some(dir),
            Err(Error::Store(store::Error::BranchNotFound)) => None,
            Err(error) => return Err(error),
        };

        let dir = if let Some(dir) = dir {
            dir
        } else {
            let branch_id = *branch.id();

            let mut dir = Self::create(lock, branch, blob_id, None);
            dir.save(&mut tx, &mut changeset, &Content::empty()).await?;

            if initial_version > 0 {
                dir.bump_self(
                    &mut tx,
                    &mut changeset,
                    &VersionVector::single(branch_id, initial_version),
                )
                .await?;
            }

            dir.commit(tx, changeset).await?;
            dir
        };

        Ok(dir)
    }

    /// Reloads this directory from the db.
    pub(crate) async fn refresh(&mut self) -> Result<()> {
        let mut tx = self.branch().store().begin_read().await?;
        self.refresh_in(&mut tx).await
    }

    /// Lookup an entry of this directory by name.
    pub fn lookup<'a>(&'a self, name: &'a str) -> Result<EntryRef<'a>> {
        self.content
            .get_key_value(name)
            .map(|(name, data)| EntryRef::new(self, name, data))
            .ok_or(Error::EntryNotFound)
    }

    /// Returns iterator over the entries of this directory.
    pub fn entries(&self) -> impl DoubleEndedIterator<Item = EntryRef<'_>> + Clone {
        self.content
            .iter()
            .map(move |(name, data)| EntryRef::new(self, name, data))
    }

    /// Creates a new file inside this directory.
    pub async fn create_file(&mut self, name: String) -> Result<File> {
        let mut tx = self.branch().store().begin_write().await?;
        let mut changeset = Changeset::new();

        self.refresh_in(&mut tx).await?;

        let bump = tx.version_vector_bump(self.branch().id()).await?;

        let blob_id = rand::random();
        let version_vector = self.content.initial_version_vector(&name).merged(&bump);
        let data = EntryData::file(blob_id, version_vector);
        let parent = self.create_parent_context(name.clone());

        let mut file = File::create(self.branch().clone(), Locator::head(blob_id), parent);
        let mut content = self.content.clone();

        content.insert(name, data)?;

        file.save(&mut tx, &mut changeset).await?;
        self.save(&mut tx, &mut changeset, &content).await?;
        self.bump_all(&mut tx, &mut changeset, &bump).await?;
        self.commit(tx, changeset).await?;
        self.finalize(content);

        Ok(file)
    }

    /// Creates a new subdirectory of this directory.
    #[instrument(skip(self))]
    pub(crate) async fn create_directory(&mut self, name: String) -> Result<Self> {
        let blob_id = rand::random();
        let lock = self
            .branch()
            .locker()
            .try_read(blob_id)
            .map_err(|_| Error::EntryExists)?;

        let mut tx = self.branch().store().begin_write().await?;
        let mut changeset = Changeset::new();

        self.refresh_in(&mut tx).await?;

        let bump = tx.version_vector_bump(self.branch().id()).await?;

        let (dir, content) = self
            .create_directory_in(lock, &mut tx, &mut changeset, name, blob_id, &bump)
            .await?;

        self.bump_all(&mut tx, &mut changeset, &bump).await?;
        self.commit(tx, changeset).await?;
        self.finalize(content);

        Ok(dir)
    }

    async fn create_directory_in(
        &mut self,
        lock: ReadLock,
        tx: &mut WriteTransaction,
        changeset: &mut Changeset,
        name: String,
        blob_id: BlobId,
        version_vector: &VersionVector,
    ) -> Result<(Self, Content)> {
        let version_vector = self
            .content
            .initial_version_vector(&name)
            .merged(version_vector);
        let data = EntryData::directory(blob_id, version_vector);
        let parent = self.create_parent_context(name.clone());

        let mut dir = Directory::create(lock, self.branch().clone(), blob_id, Some(parent));
        let mut content = self.content.clone();

        content.insert(name, data)?;

        dir.save(tx, changeset, &Content::empty()).await?;
        self.save(tx, changeset, &content).await?;

        Ok((dir, content))
    }

    fn create_parent_context(&self, entry_name: String) -> ParentContext {
        ParentContext::new(
            *self.blob_id(),
            self.lock.clone(),
            entry_name,
            self.parent.clone(),
        )
    }

    /// Removes a file or subdirectory from this directory. If the entry to be removed is a
    /// directory, it needs to be empty or a `DirectoryNotEmpty` error is returned.
    ///
    /// `branch_id` and `version_vector` specify the entry to be removed. This makes it possible to
    /// remove entries from remote branches.
    ///
    /// In most cases the removal is implemented by inserting a "tombstone" in place of the entry.
    /// One exception is when removing a remote entry which also has a concurrent local version. In
    /// that case the version vector of the local entry is bumped to be happens-after the one to be
    /// removed. This effectively removes the remote entry while keeping the local one.
    #[instrument(skip(self))]
    pub(crate) async fn remove_entry(
        &mut self,
        name: &str,
        branch_id: &PublicKey,
        version_vector: &VersionVector,
    ) -> Result<()> {
        let mut tx = self.branch().store().begin_write().await?;
        let mut changeset = Changeset::new();

        self.refresh_in(&mut tx).await?;

        // If we are removing a directory, ensure it's empty (recursive removal can still be
        // implemented at the upper layers).
        self.check_directory_empty(&mut tx, name).await?;

        // Note the ancestors are bumped only with the local version. The tombstone also merges the
        // version vector of the removed entry (so it's happens-after it) but that must not leak
        // into the ancestors because it might come from a remote branch that hasn't been fully
        // merged yet.
        let bump = tx.version_vector_bump(self.branch().id()).await?;
        self.bump_all(&mut tx, &mut changeset, &bump).await?;

        let content = self
            .begin_remove_entry(
                &mut tx,
                &mut changeset,
                name,
                branch_id,
                bump.merged(version_vector),
                TombstoneCause::Removed,
            )
            .await?;

        self.commit(tx, changeset).await?;
        self.finalize(content);

        Ok(())
    }

    /// Creates a tombstone for entry with the given name. If the entry exists, this effectively
    /// removes it. If it doesn't exist, it still creates the tombstone.
    ///
    /// If `tombstone` is happens-before the existing entry (if any), this operation has no effect
    /// but it's not an error.
    ///
    /// This method is meant to be used during the merge algorithm which is why it *doesn't* bump
    /// the version vector of this directory (or any of its ancestors). For removing entries
    /// locally, use [`Self::remove_entry`] instead.
    #[instrument()]
    pub(crate) async fn create_tombstone(
        &mut self,
        name: &str,
        tombstone: EntryTombstoneData,
    ) -> Result<()> {
        let mut tx = self.branch().store().begin_write().await?;
        let mut changeset = Changeset::new();

        self.refresh_in(&mut tx).await?;

        let content = match self
            .begin_insert_entry(
                &mut tx,
                &mut changeset,
                name.to_owned(),
                EntryData::Tombstone(tombstone),
            )
            .await
        {
            Ok(content) => content,
            Err(Error::EntryExists) => return Ok(()),
            Err(error) => return Err(error),
        };

        self.commit(tx, changeset).await?;
        self.finalize(content);

        Ok(())
    }

    /// Moves an entry at `src_name` from this directory to the `dst_dir` directory at `dst_name`.
    ///
    /// It adds a tombstone to where the entry is being moved from and creates a new entry at the
    /// destination.
    ///
    /// If an entry already exists at the destination, it's replaced, following the semantics of
    /// POSIX `rename`: a file (or a tombstone) can be replaced by a file and an empty directory (or
    /// a tombstone) can be replaced by a directory. Otherwise an error is returned
    /// (`EntryIsDirectory`, `EntryIsFile` or `DirectoryNotEmpty`). Note the destination is looked
    /// up inside the same transaction as the move, so whatever is at the destination at the time
    /// of the move gets replaced.
    ///
    /// To move an entry within the same directory, clone `self` and pass it as `dst_dir`.
    ///
    /// # Cancel safety
    ///
    /// This function is atomic and thus cancel safe. Either the entry is both removed from the src
    /// directory and inserted into the dst directory or, in case of error or cancellation, none of
    /// those operations happen.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `dst_dir` are from different branches.
    pub(crate) async fn move_entry(
        &mut self,
        src_name: &str,
        dst_dir: &mut Directory,
        dst_name: &str,
    ) -> Result<()> {
        assert_eq!(
            self.branch().id(),
            dst_dir.branch().id(),
            "can't move entry between branches"
        );

        let mut tx = self.branch().store().begin_write().await?;

        let src_dir = self;

        src_dir.refresh_in(&mut tx).await?;
        dst_dir.refresh_in(&mut tx).await?;

        let bump = tx.version_vector_bump(src_dir.branch().id()).await?;

        let mut data = src_dir.lookup(src_name)?.clone_data();

        let dst_vv = dst_dir
            .replaced_version_vector(&mut tx, &data, dst_name)
            .await?
            .merged(&bump);
        let src_vv = mem::replace(data.version_vector_mut(), dst_vv).merged(&bump);

        let mut changeset = Changeset::new();
        let dst_content = dst_dir
            .begin_insert_entry(&mut tx, &mut changeset, dst_name.to_owned(), data)
            .await?;
        dst_dir.bump_all(&mut tx, &mut changeset, &bump).await?;

        changeset
            .apply(
                &mut tx,
                src_dir.branch().id(),
                src_dir
                    .branch()
                    .keys()
                    .write()
                    .ok_or(Error::PermissionDenied)?,
            )
            .await?;

        src_dir.refresh_in(&mut tx).await?;

        let mut changeset = Changeset::new();
        let branch_id = *src_dir.branch().id();
        let src_content = src_dir
            .begin_remove_entry(
                &mut tx,
                &mut changeset,
                src_name,
                &branch_id,
                src_vv,
                TombstoneCause::Moved,
            )
            .await?;

        src_dir.bump_all(&mut tx, &mut changeset, &bump).await?;

        src_dir.commit(tx, changeset).await?;

        src_dir.finalize(src_content);
        dst_dir.finalize(dst_content);

        Ok(())
    }

    /// Forks this directory (but not its content) into `dst_branch`. This effectively creates an
    /// empty directory in `dst_branch` at the same path as `self`. If the dst directory already
    /// exists, it only updates it's version vector and possibly blob_id.
    ///
    /// Note this implicitly forks all the ancestor directories first.
    ///
    /// Note this method *doesn't* bump the version vector of this directory (or any of its
    /// ancestors) because it's only meant to be used for merging or to prepare remote entries for
    /// local mutations.
    #[instrument(name = "fork directory", skip_all)]
    #[async_recursion]
    pub(crate) async fn fork(&self, dst_branch: &Branch) -> Result<Directory> {
        if dst_branch.id() == self.branch().id() {
            return Ok(self.clone());
        }

        let (parent, src_vv) = self.prepare_fork().await?;

        if let Some((parent_dir, entry_name)) = parent {
            let mut parent_dir = parent_dir.fork(dst_branch).await?;
            let blob_id = *self.blob_id();

            parent_dir.fork_into(entry_name, blob_id, src_vv).await
        } else {
            Self::open_or_create_root(dst_branch.clone(), 0).await
        }
    }

    /// Finalize merge into this directory by updating its version vector.
    ///
    /// Note this method bumps the version vector of this directory but not of any of its ancestors
    /// because it's only meant to be used for merging.
    pub(crate) async fn finalize_merge(&mut self, bump: &VersionVector) -> Result<()> {
        let mut tx = self.branch().store().begin_write().await?;
        let mut changeset = Changeset::new();

        self.refresh_in(&mut tx).await?;
        self.bump_self(&mut tx, &mut changeset, bump).await?;
        self.commit(tx, changeset).await?;

        Ok(())
    }

    /// Prepares information needed to fork this directory.
    ///
    /// Returns the parent directory and entry name (unless root) and the current version vector of
    /// this directory
    async fn prepare_fork(&self) -> Result<(Option<(Self, &str)>, VersionVector)> {
        let mut tx = self.branch().store().begin_read().await?;

        if let Some(parent) = &self.parent {
            let parent_dir = parent.open_in(&mut tx, self.branch().clone()).await?;
            let entry_name = parent.entry_name();
            let current_vv = parent_dir.lookup(entry_name)?.version_vector().clone();

            Ok((Some((parent_dir, entry_name)), current_vv))
        } else {
            let current_vv = tx
                .load_latest_approved_root_node(self.branch().id(), RootNodeFilter::Any)
                .await?
                .proof
                .into_version_vector();

            Ok((None, current_vv))
        }
    }

    /// Forks a directory from a remote branch into the subdirectory at `name` in this directory.
    async fn fork_into(
        &mut self,
        name: &str,
        src_blob_id: BlobId,
        src_vv: VersionVector,
    ) -> Result<Self> {
        let new_lock = self.branch().locker().read(src_blob_id).await;
        let (mut tx, old_lock, old_vv) = self.begin_fork(name).await?;
        let mut changeset = Changeset::new();

        let (dir, content) = if let Some(old_lock) = old_lock {
            // Select which blob_id to use for the forked directory.
            let new_lock = match src_vv.partial_cmp(&old_vv) {
                Some(Ordering::Greater) => new_lock,
                Some(Ordering::Less) => old_lock.clone(),
                Some(Ordering::Equal) | None => {
                    // Break ties by arbitrarily taking the greater one. This assures that every
                    // replica picks the same blob_id.
                    if new_lock.blob_id() > old_lock.blob_id() {
                        new_lock
                    } else {
                        old_lock.clone()
                    }
                }
            };

            self.fork_update(&mut tx, &mut changeset, name, old_lock, new_lock)
                .await?
        } else {
            self.create_directory_in(
                new_lock,
                &mut tx,
                &mut changeset,
                name.to_owned(),
                src_blob_id,
                &VersionVector::new(),
            )
            .await?
        };

        self.commit(tx, changeset).await?;
        self.finalize(content);

        Ok(dir)
    }

    /// Begins the forking operation into the subdirectory at `name`.
    /// Returns the write transaction, the read lock for the currently existing directory at `name`
    /// (if any) and its current version vector.
    async fn begin_fork(
        &mut self,
        name: &str,
    ) -> Result<(WriteTransaction, Option<ReadLock>, VersionVector)> {
        let mut old_blob_id = match self.lookup(name) {
            Ok(EntryRef::Directory(entry)) => Some(*entry.blob_id()),
            Ok(EntryRef::File(_) | EntryRef::Tombstone(_)) | Err(Error::EntryNotFound) => None,
            Err(error) => return Err(error),
        };

        loop {
            let old_lock = if let Some(old_blob_id) = old_blob_id {
                Some(self.branch().locker().read(old_blob_id).await)
            } else {
                None
            };

            let mut tx = self.branch().store().begin_write().await?;

            self.refresh_in(&mut tx).await?;

            match (self.lookup(name), old_lock) {
                (Ok(EntryRef::Directory(entry)), Some(old_lock))
                    if entry.blob_id() == old_lock.blob_id() =>
                {
                    return Ok((tx, Some(old_lock), entry.version_vector().clone()));
                }
                (Ok(EntryRef::Directory(entry)), _) => {
                    old_blob_id = Some(*entry.blob_id());
                    continue;
                }
                (Ok(EntryRef::Tombstone(_)) | Err(Error::EntryNotFound), None) => {
                    return Ok((tx, None, VersionVector::new()));
                }
                (Ok(EntryRef::Tombstone(_)) | Err(Error::EntryNotFound), Some(_)) => {
                    old_blob_id = None;
                    continue;
                }
                // Can't fork directory over existing file
                (Ok(EntryRef::File(_)), _) => return Err(Error::EntryExists),
                (Err(error), _) => return Err(error),
            }
        }
    }

    /// Forks into an existing subdirectory.
    ///
    /// # Panics
    ///
    /// Panics if the entry at `name` doesn't exist or is not a directory.
    async fn fork_update(
        &mut self,
        tx: &mut WriteTransaction,
        changeset: &mut Changeset,
        name: &str,
        old_lock: ReadLock,
        new_lock: ReadLock,
    ) -> Result<(Self, Content)> {
        let old_blob_id = *old_lock.blob_id();
        let new_blob_id = *new_lock.blob_id();

        let parent_context = self.create_parent_context(name.to_owned());

        let mut dir = Self::open_in(
            Some(old_lock),
            tx,
            self.branch().clone(),
            old_blob_id,
            Some(parent_context),
            DirectoryFallback::Disabled,
        )
        .await?;

        let mut self_content = self.content.clone();

        let entry = match self_content.get_mut(name) {
            Some(EntryData::Directory(entry)) => entry,
            Some(EntryData::File(_) | EntryData::Tombstone(_)) | None => unreachable!(),
        };

        // Change the blob id
        if new_blob_id != entry.blob_id {
            // Replace and remove the old blob.
            //
            // Note the removal is not strictly necessary because the old blob would be eventually
            // removed by the garbage collector, but doing it this way improves merge performance:
            //
            // This way, when merging two branches, A and B, where A is happens-after B, both
            // branches become identical (same vv, same hash) right after the merge. If we delegated
            // the removal to the gc, they would not be immediately identical - because the orphaned
            // blob would exist in one but not the other - and would only become so after the gc
            // run. This avoids some unnecessary message exchanges during syncing.
            mem::replace(
                &mut dir.blob,
                Blob::create(self.branch().clone(), new_blob_id),
            )
            .remove(changeset);

            dir.lock = Some(new_lock);

            let dir_content = mem::replace(&mut dir.content, Content::empty());
            dir.save(tx, changeset, &dir_content).await?;

            entry.blob_id = new_blob_id;
        }

        self.save(tx, changeset, &self_content).await?;

        Ok((dir, self_content))
    }

    pub(crate) async fn parent(&self) -> Result<Option<Directory>> {
        if let Some(parent) = &self.parent {
            Ok(Some(parent.open(self.branch().clone()).await?))
        } else {
            Ok(None)
        }
    }

    async fn open_in(
        lock: Option<ReadLock>,
        tx: &mut ReadTransaction,
        branch: Branch,
        blob_id: BlobId,
        parent: Option<ParentContext>,
        fallback: DirectoryFallback,
    ) -> Result<Self> {
        let (blob, content) = load(tx, branch, blob_id, fallback).await?;

        Ok(Self {
            blob,
            parent,
            content,
            lock,
        })
    }

    async fn open_snapshot(
        tx: &mut ReadTransaction,
        branch: Branch,
        locator: Locator,
        fallback: DirectoryFallback,
    ) -> Result<Content> {
        let (_, content) = load(tx, branch, *locator.blob_id(), fallback).await?;
        Ok(content)
    }

    async fn open(
        branch: Branch,
        blob_id: BlobId,
        parent: Option<ParentContext>,
        locking: DirectoryLocking,
        fallback: DirectoryFallback,
    ) -> Result<Self> {
        let lock = match locking {
            DirectoryLocking::Enabled => Some(branch.locker().read(blob_id).await),
            DirectoryLocking::Disabled => None,
        };

        let mut tx = branch.store().begin_read().await?;
        Self::open_in(lock, &mut tx, branch, blob_id, parent, fallback).await
    }

    fn create(
        lock: ReadLock,
        branch: Branch,
        blob_id: BlobId,
        parent: Option<ParentContext>,
    ) -> Self {
        let blob = Blob::create(branch, blob_id);

        Directory {
            blob,
            parent,
            content: Content::empty(),
            lock: Some(lock),
        }
    }

    #[async_recursion]
    pub async fn debug_print(&self, print: DebugPrinter) {
        for (name, entry_data) in &self.content {
            print.display(&format_args!("{name:?}: {entry_data:?}"));

            match entry_data {
                EntryData::File(file_data) => {
                    let print = print.indent();

                    let parent_context = self.create_parent_context(name.into());
                    let file = File::open(
                        self.blob.branch().clone(),
                        Locator::head(file_data.blob_id),
                        parent_context,
                    )
                    .await;

                    match file {
                        Ok(mut file) => {
                            let mut buf = [0; 32];
                            let length_result = file.read(&mut buf).await;
                            match length_result {
                                Ok(length) => {
                                    let file_len = file.len();
                                    let ellipsis = if file_len > length as u64 { ".." } else { "" };
                                    print.display(&format!(
                                        "Content: {:?}{}",
                                        std::str::from_utf8(&buf[..length]),
                                        ellipsis
                                    ));
                                }
                                Err(e) => {
                                    print.display(&format!("Failed to read {e:?}"));
                                }
                            }
                        }
                        Err(e) => {
                            print.display(&format!("Failed to open {e:?}"));
                        }
                    }
                }
                EntryData::Directory(data) => {
                    let print = print.indent();

                    let parent_context = self.create_parent_context(name.into());
                    let dir = Directory::open(
                        self.blob.branch().clone(),
                        data.blob_id,
                        Some(parent_context),
                        DirectoryLocking::Disabled,
                        DirectoryFallback::Disabled,
                    )
                    .await;

                    match dir {
                        Ok(dir) => {
                            dir.debug_print(print).await;
                        }
                        Err(e) => {
                            print.display(&format!("Failed to open {e:?}"));
                        }
                    }
                }
                EntryData::Tombstone(_) => {}
            }
        }
    }

    /// Branch of this directory
    pub fn branch(&self) -> &Branch {
        self.blob.branch()
    }

    /// Blob id of this directory
    pub(crate) fn blob_id(&self) -> &BlobId {
        self.blob.id()
    }

    /// Length of this directory in bytes. Does not include the content, only the size of directory
    /// itself.
    pub fn len(&self) -> u64 {
        self.blob.len()
    }

    pub(crate) async fn version_vector(&self) -> Result<VersionVector> {
        if let Some(parent) = &self.parent {
            parent.entry_version_vector(self.branch().clone()).await
        } else {
            self.branch().version_vector().await
        }
    }

    async fn begin_remove_entry(
        &mut self,
        tx: &mut ReadTransaction,
        changeset: &mut Changeset,
        name: &str,
        branch_id: &PublicKey,
        new_vv: VersionVector,
        cause: TombstoneCause,
    ) -> Result<Content> {
        let new_data = match self.lookup(name) {
            Ok(old_entry) if branch_id != self.branch().id() => {
                let mut new_data = old_entry.clone_data();
                new_data.version_vector_mut().merge(&new_vv);
                new_data
            }
            Ok(_) | Err(Error::EntryNotFound) => {
                EntryData::Tombstone(EntryTombstoneData::new(cause, new_vv))
            }
            Err(error) => return Err(error),
        };

        self.begin_insert_entry(tx, changeset, name.to_owned(), new_data)
            .await
    }

    /// Checks whether the entry at `name` can be replaced by `new_data` (following the semantics of
    /// POSIX `rename`) and returns its version vector (or empty version vector if there is no entry
    /// at `name`).
    async fn replaced_version_vector(
        &self,
        tx: &mut ReadTransaction,
        new_data: &EntryData,
        name: &str,
    ) -> Result<VersionVector> {
        let old_vv = match (new_data, self.lookup(name)) {
            (EntryData::Tombstone(_), _) => return Err(Error::EntryNotFound),
            (_, Ok(EntryRef::Tombstone(old_entry))) => old_entry.version_vector().clone(),
            (_, Err(Error::EntryNotFound)) => VersionVector::new(),
            (_, Err(error)) => return Err(error),
            (EntryData::File(_), Ok(EntryRef::File(old_entry))) => {
                old_entry.version_vector().clone()
            }
            (EntryData::Directory(_), Ok(EntryRef::Directory(old_entry))) => {
                let old_vv = old_entry.version_vector().clone();
                self.check_directory_empty(tx, name).await?;
                old_vv
            }
            (EntryData::File(_), Ok(EntryRef::Directory(_))) => {
                return Err(Error::EntryIsDirectory);
            }
            (EntryData::Directory(_), Ok(EntryRef::File(_))) => return Err(Error::EntryIsFile),
        };

        Ok(old_vv)
    }

    async fn check_directory_empty(&self, tx: &mut ReadTransaction, name: &str) -> Result<()> {
        match self.lookup(name) {
            Ok(EntryRef::Directory(entry)) => {
                if entry
                    .open_snapshot(tx, DirectoryFallback::Disabled)
                    .await?
                    .iter()
                    .all(|(_, data)| matches!(data, EntryData::Tombstone(_)))
                {
                    Ok(())
                } else {
                    Err(Error::DirectoryNotEmpty)
                }
            }
            Ok(_) | Err(Error::EntryNotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn begin_insert_entry(
        &mut self,
        tx: &mut ReadTransaction,
        changeset: &mut Changeset,
        name: String,
        data: EntryData,
    ) -> Result<Content> {
        let mut content = self.content.clone();
        content.insert(name, data)?;
        self.save(tx, changeset, &content).await?;

        Ok(content)
    }

    async fn refresh_in(&mut self, tx: &mut ReadTransaction) -> Result<()> {
        if self.blob.is_dirty() {
            Ok(())
        } else {
            let (blob, content) = self.load(tx, DirectoryFallback::Disabled).await?;
            self.blob = blob;
            self.content = content;

            Ok(())
        }
    }

    async fn load(
        &self,
        tx: &mut ReadTransaction,
        fallback: DirectoryFallback,
    ) -> Result<(Blob, Content)> {
        load(tx, self.branch().clone(), *self.blob_id(), fallback).await
    }

    async fn save(
        &mut self,
        tx: &mut ReadTransaction,
        changeset: &mut Changeset,
        content: &Content,
    ) -> Result<()> {
        // Save the directory content into the store
        let buffer = content.serialize();
        self.blob.truncate(0)?;
        self.blob.write_all(tx, changeset, &buffer).await?;
        self.blob.flush(tx, changeset).await?;

        Ok(())
    }

    /// Atomically apply the changeset, commit the transaction and send a notification event.
    async fn commit(&mut self, mut tx: WriteTransaction, changeset: Changeset) -> Result<()> {
        let changed = changeset
            .apply(
                &mut tx,
                self.branch().id(),
                self.branch()
                    .keys()
                    .write()
                    .ok_or(Error::PermissionDenied)?,
            )
            .await?;

        if !changed {
            return Ok(());
        }

        let event_tx = self.branch().notify();
        tx.commit_and_then(move || event_tx.send()).await?;

        Ok(())
    }

    /// Updates the version vector of this directory but not its ancestors.
    async fn bump_self(
        &mut self,
        tx: &mut ReadTransaction,
        changeset: &mut Changeset,
        bump: &VersionVector,
    ) -> Result<()> {
        if bump.is_empty() {
            return Ok(());
        }

        if let Some(parent) = &self.parent {
            let mut parent_dir = parent.open_in(tx, self.branch().clone()).await?;
            let mut content = parent_dir.content.clone();
            content.bump(parent.entry_name(), bump)?;
            parent_dir.save(tx, changeset, &content).await?;
        } else {
            changeset.bump(bump.clone())
        }

        Ok(())
    }

    /// Updates the version vectors of this directory and all its ancestors.
    pub(crate) async fn bump_all(
        &mut self,
        tx: &mut ReadTransaction,
        changeset: &mut Changeset,
        bump: &VersionVector,
    ) -> Result<()> {
        if bump.is_empty() {
            return Ok(());
        }

        self.bump_self(tx, changeset, bump).await?;

        let mut curr = None;
        let mut curr_ref = self;

        while let Some(parent) = &curr_ref.parent {
            let next = parent.open_in(tx, curr_ref.branch().clone()).await?;
            curr_ref = curr.insert(next);
            curr_ref.bump_self(tx, changeset, bump).await?;
        }

        Ok(())
    }

    /// Finalize pending modifications. Call this only after the db transaction has been committed.
    fn finalize(&mut self, content: Content) {
        self.content = content;
    }
}

impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Directory")
            .field(
                "name",
                &self
                    .parent
                    .as_ref()
                    .map(|parent| parent.entry_name())
                    .unwrap_or("/"),
            )
            .field("branch", self.branch().id())
            .field("blob_id", self.blob_id())
            .finish()
    }
}

/// Enable/disable fallback to previous snapshots in case of missing blocks.
#[derive(Clone, Copy)]
pub(crate) enum DirectoryFallback {
    Enabled,
    Disabled,
}

/// Enable/disable acquiring read lock before creating/opening the directory.
#[derive(Clone, Copy)]
pub(crate) enum DirectoryLocking {
    Enabled,
    Disabled,
}

// Loads a directory. Returns the directory content and the directory blob.
//
// On missing block(s), if `fallback` is `Enabled`, falls back to previous snapshot (if any).
// Otherwise returns `BlockNotFound`.
async fn load(
    tx: &mut ReadTransaction,
    branch: Branch,
    blob_id: BlobId,
    fallback: DirectoryFallback,
) -> Result<(Blob, Content)> {
    let mut root_node = tx
        .load_latest_approved_root_node(branch.id(), RootNodeFilter::Any)
        .await?;

    loop {
        let error = match load_at(tx, &root_node, branch.clone(), blob_id).await {
            Ok((blob, content)) => return Ok((blob, content)),
            Err(error @ Error::Store(store::Error::BlockNotFound)) => error,
            Err(error) => return Err(error),
        };

        match fallback {
            DirectoryFallback::Enabled => (),
            DirectoryFallback::Disabled => return Err(error),
        }

        if let Some(prev) = tx.load_prev_approved_root_node(&root_node).await? {
            root_node = prev;
        } else {
            return Err(error);
        }
    }
}

async fn load_at(
    tx: &mut ReadTransaction,
    root_node: &RootNode,
    branch: Branch,
    blob_id: BlobId,
) -> Result<(Blob, Content)> {
    let mut blob = Blob::open_at(tx, root_node, branch, blob_id).await?;
    let buffer = blob.read_to_end_at(tx, root_node).await?;
    let content = Content::deserialize(&buffer)?;

    Ok((blob, content))
}
