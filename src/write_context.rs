use crate::{
    blob::Blob, branch::Branch, directory::Directory, entry_type::EntryType, error::Result,
    replica_id::ReplicaId,
    locator::Locator, path,
};
use camino::{Utf8Component, Utf8PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::ops::DerefMut;

/// Context needed for updating all necessary info when writing to a file or directory.
pub struct WriteContext {
    local_branch_id: ReplicaId,
    inner: Mutex<Inner>,
}

struct Inner {
    path: Utf8PathBuf,
    local_branch: Branch,
    ancestors: Vec<Directory>,
}

impl WriteContext {
    pub fn new(path: Utf8PathBuf, local_branch: Branch) -> Arc<Self> {
        Arc::new(Self {
            local_branch_id: *local_branch.id(),
            inner: Mutex::new(Inner {
                path,
                local_branch,
                ancestors: Vec::new(),
            })
        })
    }

    pub async fn child(&self, name: &str) -> Arc<Self> {
        let inner = self.inner.lock().await;

        Arc::new(Self {
            local_branch_id: self.local_branch_id,
            inner: Mutex::new(Inner {
                path: inner.path.join(name),
                local_branch: inner.local_branch.clone(),
                ancestors: Vec::new(),
            })
        })
    }

    pub async fn path(&self) -> Utf8PathBuf {
        self.inner.lock().await.path.clone()
    }

    pub fn local_branch_id(&self) -> &ReplicaId {
        &self.local_branch_id
    }

    /// Begin writing to the given blob. This ensures the blob lives in the local branch and all
    /// its ancestor directories exist and live in the local branch as well.
    /// Call `commit` to finalize the write.
    pub async fn begin(&self, entry_type: EntryType, blob: &mut Blob) -> Result<()> {
        // TODO: load the directories always

        let mut guard = self.inner.lock().await;
        let inner = guard.deref_mut();

        if blob.branch().id() == inner.local_branch.id() {
            // Blob already lives in the local branch. We assume the ancestor directories have been
            // already created as well so there is nothing else to do.
            return Ok(());
        }

        let dst_locator = if let Some((parent, name)) = path::decompose(&inner.path) {
            inner.ancestors = inner.local_branch.ensure_directory_exists(parent).await?;
            inner.ancestors
                .last_mut()
                .unwrap()
                .insert_entry(name.to_owned(), entry_type).await?
        } else {
            // `blob` is the root directory.
            Locator::Root
        };

        blob.fork(inner.local_branch.data().clone(), dst_locator)
            .await
    }

    /// Commit writing to the blob started by a previous call to `begin`. Does nothing if `begin`
    /// was not called.
    pub async fn commit(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let inner = guard.deref_mut(); 
        let mut dirs = inner.ancestors.drain(..).rev();

        for component in inner.path.components().rev() {
            match component {
                Utf8Component::Normal(name) => {
                    if let Some(mut dir) = dirs.next() {
                        dir.increment_entry_version(name)?;
                        todo!();
                        //dir.write().await?;
                    } else {
                        break;
                    }
                }
                Utf8Component::Prefix(_) | Utf8Component::RootDir | Utf8Component::CurDir => (),
                Utf8Component::ParentDir => panic!("non-normalized paths not supported"),
            }
        }

        Ok(())
    }
}

//impl Clone for WriteContext {
//    fn clone(&self) -> Self {
//        Self {
//            path: self.path.clone(),
//            local_branch: self.local_branch.clone(),
//            ancestors: Vec::new(), // The clone is produced in non-begun state.
//        }
//    }
//}
