//! Directory content

use super::entry_data::EntryData;
use crate::{
    blob::lock::UniqueLock,
    blob::BlobId,
    branch::Branch,
    error::{Error, Result},
    version_vector::VersionVector,
};
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{
        btree_map::{self, Entry},
        BTreeMap,
    },
};

/// Version of the Directory serialization format.
pub(crate) const VERSION: u64 = 2;

#[derive(Clone, Debug)]
pub(super) struct Content {
    entries: v2::Entries,
}

impl Content {
    pub fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn deserialize(mut input: &[u8]) -> Result<Self> {
        let version = vint64::decode(&mut input).map_err(|_| Error::MalformedDirectory)?;
        let entries = match version {
            VERSION => deserialize_entries(input),
            1 => Ok(v2::from_v1(deserialize_entries(input)?)),
            0 => Ok(v2::from_v1(v1::from_v0(deserialize_entries(input)?))),
            _ => Err(Error::StorageVersionMismatch),
        };

        Ok(Self { entries: entries? })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(vint64::encode(VERSION).as_ref());
        bincode::serialize_into(&mut output, &self.entries)
            .expect("failed to serialize directory content");
        output
    }

    pub fn iter(&self) -> btree_map::Iter<String, EntryData> {
        self.entries.iter()
    }

    pub fn get_key_value(&self, name: &str) -> Option<(&String, &EntryData)> {
        self.entries.get_key_value(name)
    }

    /// Inserts an entry into this directory.
    pub fn insert(
        &mut self,
        branch: &Branch,
        name: String,
        new_data: EntryData,
        lock: Option<UniqueLock>,
    ) -> Result<Option<UniqueLock>, InsertError> {
        match self.entries.entry(name) {
            Entry::Vacant(entry) => {
                assert!(lock.is_none());
                entry.insert(new_data);
                Ok(None)
            }
            Entry::Occupied(mut entry) => {
                let old_id = check_replace(entry.get(), &new_data).map_err(InsertError::Exists)?;

                let lock = match (old_id, lock) {
                    (Some(old_id), Some(lock)) => {
                        assert_eq!(lock.blob_id(), &old_id);
                        assert_eq!(lock.branch_id(), branch.id());
                        Some(lock)
                    }
                    (Some(old_id), None) => Some(
                        branch
                            .locker()
                            .try_unique(old_id)
                            .map_err(|_| InsertError::Locked)?,
                    ),
                    (None, None) => None,
                    (None, Some(_)) => panic!("unexpected lock for non-existing entry"),
                };

                entry.insert(new_data);
                Ok(lock)
            }
        }
    }

    /// Check whether an entry can be inserted into this directory without actually inserting it.
    pub fn check_insert(
        &self,
        name: &str,
        new_data: &EntryData,
    ) -> Result<Option<BlobId>, EntryExists> {
        if let Some(old_data) = self.entries.get(name) {
            check_replace(old_data, new_data)
        } else {
            Ok(None)
        }
    }

    /// Updates the version vector of entry at `name`.
    pub fn bump(&mut self, branch: &Branch, name: &str, merge: &VersionVector) -> Result<()> {
        let vv = self
            .entries
            .get_mut(name)
            .ok_or(Error::EntryNotFound)?
            .version_vector_mut();

        if merge.is_empty() {
            vv.increment(*branch.id());
        } else {
            vv.merge(merge);
        }

        Ok(())
    }

    /// Initial version vector for a new entry to be inserted.
    pub fn initial_version_vector(&self, name: &str) -> VersionVector {
        if let Some(EntryData::Tombstone(entry)) = self.entries.get(name) {
            entry.version_vector.clone()
        } else {
            VersionVector::new()
        }
    }
}

#[derive(Debug)]
pub(crate) enum EntryExists {
    /// The existing entry is more up-to-date and points to the same blob than the one being
    /// inserted
    Same,
    /// The existing entry is either points to a different blob or is concurrent
    Different,
}

#[derive(Debug)]
pub(crate) enum InsertError {
    /// The entry exists
    Exists(EntryExists),
    /// The existing entry is locked
    Locked,
}

impl From<InsertError> for Error {
    fn from(error: InsertError) -> Self {
        match error {
            InsertError::Exists(_) => Self::EntryExists,
            InsertError::Locked => Self::Locked,
        }
    }
}

impl<'a> IntoIterator for &'a Content {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = btree_map::Iter<'a, String, EntryData>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

fn deserialize_entries<'a, T: Deserialize<'a>>(input: &'a [u8]) -> Result<T, Error> {
    bincode::deserialize(input).map_err(|_| Error::MalformedDirectory)
}

fn check_replace(old: &EntryData, new: &EntryData) -> Result<Option<BlobId>, EntryExists> {
    // Replace entries only if the new version is more up to date than the old version.

    match new.version_vector().partial_cmp(old.version_vector()) {
        Some(Ordering::Greater) => Ok(old.blob_id().copied()),
        Some(Ordering::Equal | Ordering::Less) => {
            if new.blob_id() == old.blob_id() {
                Err(EntryExists::Same)
            } else {
                Err(EntryExists::Different)
            }
        }
        None => Err(EntryExists::Different),
    }
}

mod v2 {
    use super::{
        super::entry_data::{EntryData, EntryTombstoneData, TombstoneCause},
        v1,
    };
    use std::collections::BTreeMap;

    pub(super) type Entries = BTreeMap<String, EntryData>;

    pub(super) fn from_v1(v1: v1::Entries) -> Entries {
        v1.into_iter()
            .map(|(name, data)| {
                let data = match data {
                    v1::EntryData::File(data) => EntryData::File(data),
                    v1::EntryData::Directory(data) => EntryData::Directory(data),
                    v1::EntryData::Tombstone(v1::EntryTombstoneData { version_vector }) => {
                        EntryData::Tombstone(EntryTombstoneData {
                            cause: TombstoneCause::Removed,
                            version_vector,
                        })
                    }
                };

                (name, data)
            })
            .collect()
    }
}

mod v1 {
    use super::v0;
    use std::collections::BTreeMap;
    pub(super) use v0::{EntryData, EntryTombstoneData};

    pub(super) type Entries = BTreeMap<String, v0::EntryData>;

    pub(super) fn from_v0(v0: v0::Entries) -> Entries {
        use crate::conflict;

        let mut v1 = BTreeMap::new();

        for (name, versions) in v0 {
            if versions.len() <= 1 {
                // If there is only one version, insert it directly
                if let Some(data) = versions.into_values().next() {
                    v1.insert(name, data);
                }
            } else {
                // If there is more than one version, create unique name for each of them and insert
                // them as separate entries
                for (author_id, data) in versions {
                    v1.insert(conflict::create_unique_name(&name, &author_id), data);
                }
            }
        }

        v1
    }
}

mod v0 {
    use super::super::entry_data::{EntryDirectoryData, EntryFileData};
    use crate::{crypto::sign::PublicKey, version_vector::VersionVector};
    use serde::Deserialize;
    use std::collections::BTreeMap;

    pub(super) type Entries = BTreeMap<String, BTreeMap<PublicKey, EntryData>>;

    #[derive(Deserialize)]
    pub(super) enum EntryData {
        File(EntryFileData),
        Directory(EntryDirectoryData),
        Tombstone(EntryTombstoneData),
    }

    #[derive(Deserialize)]
    pub(super) struct EntryTombstoneData {
        pub version_vector: VersionVector,
    }
}
