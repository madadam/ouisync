use crate::{registry::Handle, repository::RepositoryHolder, state::State};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

// Currently this is only a read-only snapshot of a directory.
#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Directory(Vec<DirEntry>);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DirEntry {
    pub name: String,
    pub entry_type: u8,
}

pub(crate) async fn create(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
) -> Result<(), ouisync_lib::Error> {
    state
        .get_repository(repo)
        .repository
        .create_directory(path)
        .await?;
    Ok(())
}

pub(crate) async fn open(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
) -> Result<Directory, ouisync_lib::Error> {
    let repo = state.get_repository(repo);

    let dir = repo.repository.open_directory(path).await?;
    let entries = dir
        .entries()
        .map(|entry| DirEntry {
            name: entry.unique_name().into_owned(),
            entry_type: entry.entry_type().into(),
        })
        .collect();

    Ok(Directory(entries))
}

/// Removes the directory at the given path from the repository. If `recursive` is true it removes
/// also the contents, otherwise the directory must be empty.
pub(crate) async fn remove(
    state: &State,
    repo: Handle<RepositoryHolder>,
    path: Utf8PathBuf,
    recursive: bool,
) -> Result<(), ouisync_lib::Error> {
    let repo = &state.get_repository(repo).repository;

    if recursive {
        repo.remove_entry_recursively(path).await?
    } else {
        repo.remove_entry(path).await?
    }

    Ok(())
}
