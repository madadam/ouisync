use super::*;
use crate::{
    access_control::WriteSecrets, blob::BlobCache, branch::Branch, crypto::sign::PublicKey, db,
    index::BranchData, version_vector::VersionVector,
};
use assert_matches::assert_matches;
use futures_util::future;
use rand::{rngs::StdRng, SeedableRng};
use sqlx::Connection;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::broadcast;

#[tokio::test(flavor = "multi_thread")]
async fn no_conflict() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root0, "file0.txt", &[], &branches[0]).await;

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root1, "file1.txt", &[], &branches[1]).await;

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);
    let entries: Vec<_> = root.entries().collect();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name(), "file0.txt");
    assert_eq!(entries[0].entry_type(), EntryType::File);
    assert_eq!(entries[1].name(), "file1.txt");
    assert_eq!(entries[1].entry_type(), EntryType::File);

    for (index, entry) in entries.iter().enumerate() {
        let name = format!("file{}.txt", index);

        let lookup: Vec<_> = root.lookup(&name).collect();
        assert_eq!(lookup.len(), 1);
        assert_eq!(lookup[0].name(), entry.name());

        let lookup_unique = root.lookup_unique(&name).unwrap();
        assert_eq!(lookup_unique.name(), entry.name());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_independent_files() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root0, "file.txt", &[], &branches[0]).await;

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root1, "file.txt", &[], &branches[1]).await;

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file0 = entries
        .iter()
        .find(|file| file.first_branch().id() == branches[0].id())
        .unwrap();

    let unique_name = conflict::create_unique_name("file.txt", branches[0].id());

    assert_eq!(file0.name(), "file.txt");
    assert_eq!(&file0.unique_name(), &unique_name);

    let file_ref = root.lookup_unique(&unique_name).unwrap().file().unwrap();
    assert_eq!(file_ref.name(), file0.name());
    assert_eq!(file_ref.branch().id(), branches[0].id());

    let file1 = entries
        .iter()
        .find(|file| file.first_branch().id() == branches[1].id())
        .unwrap();

    let unique_name = conflict::create_unique_name("file.txt", branches[1].id());

    assert_eq!(file1.name(), "file.txt");
    assert_eq!(&file1.unique_name(), &unique_name);

    let file_ref = root.lookup_unique(&unique_name).unwrap().file().unwrap();
    assert_eq!(file_ref.name(), file1.name());
    assert_eq!(file_ref.branch().id(), branches[1].id());

    let entries: Vec<_> = root.lookup("file.txt").collect();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .any(|entry| entry.first_branch().id() == branches[0].id()),);
    assert!(entries
        .iter()
        .any(|entry| entry.first_branch().id() == branches[1].id()));

    assert_matches!(root.lookup_unique("file.txt"), Err(Error::AmbiguousEntry));

    assert_unique_and_ordered(2, root.entries());
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_forked_files() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut tx = pool.begin().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut tx).await.unwrap();
    create_file(&mut tx, &mut root0, "file.txt", b"one", &branches[0]).await;

    // Fork the file into branch 1 and then modify it.
    let mut file1 = open_file(&mut tx, &root0, "file.txt").await;
    file1.fork(&mut tx, branches[1].clone()).await.unwrap();
    file1.write(&mut tx, b"two").await.unwrap();
    file1.flush(&mut tx).await.unwrap();

    // Modify the file by branch 0 as well, to create concurrent versions
    let mut file0 = open_file(&mut tx, &root0, "file.txt").await;
    file0.write(&mut tx, b"three").await.unwrap();
    file0.flush(&mut tx).await.unwrap();

    // Open branch 1's root dir which should have been created in the process.
    let root1 = branches[1].open_root(&mut tx).await.unwrap();

    let root = JointDirectory::new(Some(branches[1].clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    for branch in &branches {
        let file = entries
            .iter()
            .find(|entry| entry.first_branch().id() == branch.id())
            .unwrap();
        assert_eq!(file.name(), "file.txt");
        assert_eq!(
            &file.unique_name(),
            &conflict::create_unique_name("file.txt", branch.id())
        );
    }

    assert_unique_and_ordered(2, root.entries());
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_directories() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();
    root0
        .create_directory(&mut conn, "dir".to_owned())
        .await
        .unwrap();

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    root1
        .create_directory(&mut conn, "dir".to_owned())
        .await
        .unwrap();

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);

    let directories: Vec<_> = root
        .entries()
        .map(|entry| entry.directory().unwrap())
        .collect();
    assert_eq!(directories.len(), 1);
    assert_eq!(directories[0].name(), "dir");
    assert_eq!(directories[0].unique_name(), "dir");
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_file_and_single_version_directory() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();

    create_file(&mut conn, &mut root0, "config", &[], &branches[0]).await;

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    root1
        .create_directory(&mut conn, "config".to_owned())
        .await
        .unwrap();

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::File(_)))
        .unwrap();

    assert_eq!(file_entry.name(), "config");
    assert_eq!(
        &file_entry.unique_name(),
        &conflict::create_unique_name("config", branches[0].id())
    );

    let dir_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::Directory(_)))
        .unwrap();

    assert_eq!(dir_entry.name(), "config");
    assert_eq!(
        &dir_entry.unique_name(),
        &conflict::create_unique_name("config", branches[1].id())
    );

    let entries = root.lookup("config");
    assert_eq!(entries.count(), 2);

    assert_matches!(root.lookup_unique("config"), Err(Error::AmbiguousEntry));

    let name = conflict::create_unique_name("config", branches[0].id());
    let entry = root.lookup_unique(&name).unwrap();
    assert_eq!(entry.entry_type(), EntryType::File);
    assert_eq!(entry.file().unwrap().branch().id(), branches[0].id());

    let name = conflict::create_unique_name("config", branches[1].id());
    let entry = root.lookup_unique(&name).unwrap();
    assert_eq!(entry.entry_type(), EntryType::Directory);
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_file_and_multi_version_directory() {
    let (_base_dir, pool, mut branches) = setup(3).await;
    let mut conn = pool.acquire().await.unwrap();

    // Sort the branches by their ids because directory disambiguator is the lexicographically
    // first branch id.
    branches.sort_by(|lhs, rhs| lhs.id().cmp(rhs.id()));

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root0, "config", &[], &branches[0]).await;

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    root1
        .create_directory(&mut conn, "config".to_owned())
        .await
        .unwrap();

    let mut root2 = branches[2].open_or_create_root(&mut conn).await.unwrap();
    root2
        .create_directory(&mut conn, "config".to_owned())
        .await
        .unwrap();

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1, root2]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::File(_)))
        .unwrap();

    assert_eq!(
        &file_entry.unique_name(),
        &conflict::create_unique_name("config", branches[0].id())
    );

    let dir_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::Directory(_)))
        .unwrap();

    assert_eq!(
        &dir_entry.unique_name(),
        &conflict::create_unique_name("config", branches[1].id())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_identical_versions() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    // Create a file by one branch.
    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut root0, "file.txt", b"one", &branches[0]).await;

    // Fork it into the other branch, creating an identical version of it.
    let mut file1 = open_file(&mut conn, &root0, "file.txt").await;
    file1.fork(&mut conn, branches[1].clone()).await.unwrap();

    let root1 = branches[1].open_root(&mut conn).await.unwrap();

    // Create joint directory using branch 1 as the local branch.
    let root = JointDirectory::new(Some(branches[1].clone()), [root0, root1]);

    // The file appears among the entries only once...
    assert_eq!(root.entries().count(), 1);

    // ...and it is the local version.
    let file = root
        .entries()
        .next()
        .unwrap()
        .file()
        .unwrap()
        .open(&mut conn)
        .await
        .unwrap();
    assert_eq!(file.branch().id(), branches[1].id());
    drop(file);

    // The file can also be retreived using `lookup`...
    let mut versions = root.lookup("file.txt");
    assert!(versions.next().is_some());
    assert!(versions.next().is_none());

    // ...and `lookup_version` using the author branch:
    root.lookup_version("file.txt", branches[0].id()).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_open_file() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut tx = pool.begin().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut tx).await.unwrap();
    let mut root1 = branches[1].open_or_create_root(&mut tx).await.unwrap();

    let file0 = root0.create_file(&mut tx, "file.txt".into()).await.unwrap();
    let vv0 = file0.version_vector(&mut tx).await.unwrap();

    let mut file1 = file0;
    file1.fork(&mut tx, branches[1].clone()).await.unwrap();
    file1.write(&mut tx, b"foo").await.unwrap();
    file1.flush(&mut tx).await.unwrap();
    root1.refresh(&mut tx).await.unwrap();

    let vv1 = file1.version_vector(&mut tx).await.unwrap();
    assert!(vv1 > vv0);

    let _file0 = root0
        .lookup("file.txt")
        .unwrap()
        .file()
        .unwrap()
        .open(&mut tx)
        .await
        .unwrap();

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);

    // Despite file1 being newer than file0, both versions are present because file0 is open and
    // might have unflushed modifications.
    assert_eq!(root.entries().count(), 2);
    root.lookup_version("file.txt", branches[0].id()).unwrap();
    root.lookup_version("file.txt", branches[1].id()).unwrap();
}

//// TODO: test conflict_forked_directories
//// TODO: test conflict_multiple_files_and_directories
//// TODO: test conflict_file_with_name_containing_branch_prefix

#[tokio::test(flavor = "multi_thread")]
async fn cd_into_concurrent_directory() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut root0 = branches[0].open_or_create_root(&mut conn).await.unwrap();

    let mut dir0 = root0
        .create_directory(&mut conn, "pics".to_owned())
        .await
        .unwrap();
    create_file(&mut conn, &mut dir0, "dog.jpg", &[], &branches[0]).await;

    let mut root1 = branches[1].open_or_create_root(&mut conn).await.unwrap();
    let mut dir1 = root1
        .create_directory(&mut conn, "pics".to_owned())
        .await
        .unwrap();
    create_file(&mut conn, &mut dir1, "cat.jpg", &[], &branches[1]).await;

    let root = JointDirectory::new(Some(branches[0].clone()), [root0, root1]);
    let dir = root.cd(&mut conn, "pics").await.unwrap();

    let entries: Vec<_> = dir.entries().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name(), "cat.jpg");
    assert_eq!(entries[1].name(), "dog.jpg");
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_non_existing_file() {
    // 0 - local, 1 - remote
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let content = b"cat";

    // Create local root dir
    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();

    // Create remote root dir
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // Create a file in the remote root
    create_file(
        &mut conn,
        &mut remote_root,
        "cat.jpg",
        content,
        &branches[1],
    )
    .await;

    drop(conn);

    // Construct a joint directory over both root dirs and merge it.
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();

    // Verify the file now exists in the local branch.
    let local_content = open_file(&mut conn, &local_root, "cat.jpg")
        .await
        .read_to_end(&mut conn)
        .await
        .unwrap();
    assert_eq!(local_content, content);

    // Local branch is up to date
    assert!(
        local_root.version_vector(&mut conn).await.unwrap()
            >= remote_root.version_vector(&mut conn).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_older_file() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let content_v0 = b"version 0";
    let content_v1 = b"version 1";

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // Create a file in the remote root
    create_file(
        &mut conn,
        &mut remote_root,
        "cat.jpg",
        content_v0,
        &branches[1],
    )
    .await;
    drop(conn);

    // Merge to transfer the file to the local branch
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    // Modify the file by the remote branch
    let mut conn = pool.acquire().await.unwrap();
    remote_root.refresh(&mut conn).await.unwrap();
    update_file(&mut conn, &remote_root, "cat.jpg", content_v1, &branches[1]).await;
    drop(conn);

    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();
    let local_content = local_root
        .lookup("cat.jpg")
        .unwrap()
        .file()
        .unwrap()
        .open(&mut conn)
        .await
        .unwrap()
        .read_to_end(&mut conn)
        .await
        .unwrap();
    assert_eq!(local_content, content_v1);

    // Local branch is up to date
    assert!(
        local_root.version_vector(&mut conn).await.unwrap()
            >= remote_root.version_vector(&mut conn).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_newer_file() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let content_v0 = b"version 0";
    let content_v1 = b"version 1";

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    create_file(
        &mut conn,
        &mut remote_root,
        "cat.jpg",
        content_v0,
        &branches[1],
    )
    .await;
    drop(conn);

    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    // Modify the file by the local branch
    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();
    update_file(&mut conn, &local_root, "cat.jpg", content_v1, &branches[0]).await;
    drop(conn);

    JointDirectory::new(Some(branches[0].clone()), [local_root.clone(), remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let local_content = local_root
        .lookup("cat.jpg")
        .unwrap()
        .file()
        .unwrap()
        .open(&mut conn)
        .await
        .unwrap()
        .read_to_end(&mut conn)
        .await
        .unwrap();
    assert_eq!(local_content, content_v1);
}

#[tokio::test(flavor = "multi_thread")]
async fn attempt_to_merge_concurrent_file() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    create_file(&mut conn, &mut remote_root, "cat.jpg", b"v0", &branches[1]).await;
    drop(conn);

    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();
    remote_root.refresh(&mut conn).await.unwrap();

    // Modify the file by both branches concurrently
    update_file(&mut conn, &local_root, "cat.jpg", b"v1", &branches[0]).await;
    update_file(&mut conn, &remote_root, "cat.jpg", b"v2", &branches[1]).await;

    local_root.refresh(&mut conn).await.unwrap();
    remote_root.refresh(&mut conn).await.unwrap();

    assert_eq!(
        local_root
            .version_vector(&mut conn)
            .await
            .unwrap()
            .partial_cmp(&remote_root.version_vector(&mut conn).await.unwrap()),
        None
    );

    drop(conn);

    // Merge succeeds but skips over the conflicting entries.
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();

    // The local version is unchanged
    assert_eq!(
        open_file(&mut conn, &local_root, "cat.jpg")
            .await
            .read_to_end(&mut conn)
            .await
            .unwrap(),
        b"v1"
    );

    // There is still only the local version in the local branch
    assert_eq!(local_root.entries().count(), 1);

    // The branches are still concurrent
    assert_eq!(
        local_root
            .version_vector(&mut conn)
            .await
            .unwrap()
            .partial_cmp(&remote_root.version_vector(&mut conn).await.unwrap()),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_is_idempotent() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let vv0 = branches[0].version_vector(&mut conn).await.unwrap();

    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // Merge after a remote modification - this causes local modification.
    create_file(&mut conn, &mut remote_root, "cat.jpg", b"v0", &branches[1]).await;
    drop(conn);

    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let vv1 = branches[0].version_vector(&mut conn).await.unwrap();
    assert!(vv1 > vv0);
    drop(conn);

    // Merge again. This time there is no local modification because there was no remote
    // modification either.
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let vv2 = branches[0].version_vector(&mut conn).await.unwrap();
    assert_eq!(vv2, vv1);

    // Perform another remote modification and merge again - this causes local modification
    // again.
    update_file(&mut conn, &remote_root, "cat.jpg", b"v1", &branches[1]).await;
    drop(conn);

    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let vv3 = branches[0].version_vector(&mut conn).await.unwrap();
    assert!(vv3 > vv2);
    drop(conn);

    // Another idempotent merge which causes no local modification.
    JointDirectory::new(Some(branches[0].clone()), [local_root, remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let vv4 = branches[0].version_vector(&mut conn).await.unwrap();
    assert_eq!(vv4, vv3);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_create_file_roundtrip() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // remote: create the file
    create_file(&mut conn, &mut remote_root, "cat.jpg", b"v0", &branches[1]).await;
    drop(conn);

    // local: merge from remote
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let local_vv0 = branches[0].version_vector(&mut conn).await.unwrap();
    drop(conn);

    // remote: merge from local
    JointDirectory::new(
        Some(branches[1].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let remote_vv0 = branches[1].version_vector(&mut conn).await.unwrap();
    drop(conn);

    assert_eq!(local_vv0, remote_vv0);

    // local: merge from remote - this has no effect
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let local_vv1 = branches[0].version_vector(&mut conn).await.unwrap();
    drop(conn);
    assert_eq!(local_vv1, local_vv0);

    // remote: merge from local - this has no effect either
    JointDirectory::new(Some(branches[1].clone()), [local_root, remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let remote_vv1 = branches[1].version_vector(&mut conn).await.unwrap();
    assert_eq!(remote_vv1, remote_vv0);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_create_and_delete_file_roundtrip() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // local: create the file
    create_file(
        &mut conn,
        &mut local_root,
        "monkey.jpg",
        b"v0",
        &branches[0],
    )
    .await;
    drop(conn);

    // remote: merge from local
    JointDirectory::new(
        Some(branches[1].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    remote_root.refresh(&mut conn).await.unwrap();

    // remote: remove the file
    let file_vv = remote_root
        .lookup("monkey.jpg")
        .unwrap()
        .version_vector()
        .clone();
    remote_root
        .remove_entry(&mut conn, "monkey.jpg", branches[1].id(), file_vv)
        .await
        .unwrap();
    drop(conn);

    // local: merge from remote
    JointDirectory::new(
        Some(branches[0].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let local_vv = branches[0].version_vector(&mut conn).await.unwrap();
    drop(conn);

    // remote: merge from local
    JointDirectory::new(
        Some(branches[1].clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let remote_vv = branches[1].version_vector(&mut conn).await.unwrap();
    drop(conn);

    assert_eq!(local_vv, remote_vv);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_remote_only() {
    let (_base_dir, pool, branches) = setup(2).await;

    let mut conn = pool.acquire().await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();
    create_file(&mut conn, &mut remote_root, "cat.jpg", b"v0", &branches[1]).await;
    drop(conn);

    // When passing only the remote dir to the joint directory the merge still works.
    JointDirectory::new(Some(branches[0].clone()), [remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let local_root = branches[0].open_root(&mut conn).await.unwrap();
    local_root.lookup("cat.jpg").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_sequential_modifications() {
    let (_base_dir, pool, branches) = setup_with_rng(StdRng::seed_from_u64(0), 2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();

    // Create a file by local, then modify it by remote, then read it back by local verifying
    // the modification by remote got through.

    create_file(&mut conn, &mut local_root, "dog.jpg", b"v0", &branches[0]).await;
    local_root.refresh(&mut conn).await.unwrap();

    drop(conn);

    let vv0 = read_version_vector(&local_root, "dog.jpg").await;

    JointDirectory::new(
        Some(branches[1].clone()),
        [remote_root.clone(), local_root.clone()],
    )
    .merge(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    remote_root.refresh(&mut conn).await.unwrap();

    let vv1 = read_version_vector(&remote_root, "dog.jpg").await;
    assert_eq!(vv1, vv0);

    update_file(&mut conn, &remote_root, "dog.jpg", b"v1", &branches[1]).await;
    remote_root.refresh(&mut conn).await.unwrap();

    drop(conn);

    let vv2 = read_version_vector(&remote_root, "dog.jpg").await;
    assert!(vv2 > vv1);

    JointDirectory::new(Some(branches[0].clone()), [local_root.clone(), remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();

    let entry = local_root.lookup("dog.jpg").unwrap().file().unwrap();
    assert_eq!(entry.version_vector(), &vv2);

    let content = entry
        .open(&mut conn)
        .await
        .unwrap()
        .read_to_end(&mut conn)
        .await
        .unwrap();
    assert_eq!(content, b"v1");
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_concurrent_directories() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut local_dir = local_root
        .create_directory(&mut conn, "dir".into())
        .await
        .unwrap();
    create_file(&mut conn, &mut local_dir, "dog.jpg", &[], &branches[0]).await;

    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();
    let mut remote_dir = remote_root
        .create_directory(&mut conn, "dir".into())
        .await
        .unwrap();
    create_file(&mut conn, &mut remote_dir, "cat.jpg", &[], &branches[1]).await;

    drop(conn);

    JointDirectory::new(Some(branches[0].clone()), [local_root.clone(), remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();

    assert_eq!(local_root.entries().count(), 1);

    let entry = local_root.entries().next().unwrap();
    assert_eq!(entry.name(), "dir");
    assert_matches!(entry, EntryRef::Directory(_));

    // +(2, 0) for creating local "dir" and "dog.jpg"
    // +(0, 2) for creating remote "dir" and "cat.jpg"
    // +(1, 0) for modifying local "dir" when the remote "cat.jpg" is merged into it
    let expected_vv = vv![*branches[0].id() => 3, *branches[1].id() => 2];
    assert_eq!(entry.version_vector(), &expected_vv);

    let dir = entry.directory().unwrap().open(&mut conn).await.unwrap();
    assert_eq!(dir.entries().count(), 2);

    dir.lookup("dog.jpg").unwrap().file().unwrap();
    dir.lookup("cat.jpg").unwrap().file().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_file_and_tombstone() {
    // Create two branches.
    let (_base_dir, pool, branches) = setup(2).await;
    let mut conn = pool.acquire().await.unwrap();

    // Create a file in the local one.
    let mut local_root = branches[0].open_or_create_root(&mut conn).await.unwrap();
    let mut file = create_file(&mut conn, &mut local_root, "dog.jpg", &[], &branches[0]).await;
    let file_vv = file.version_vector(&mut conn).await.unwrap();

    // Fork the file into the remote branch.
    let mut remote_root = branches[1].open_or_create_root(&mut conn).await.unwrap();
    file.fork(&mut conn, branches[1].clone()).await.unwrap();

    // Remove the file in the remote branch.
    remote_root
        .remove_entry(&mut conn, "dog.jpg", branches[1].id(), file_vv)
        .await
        .unwrap();

    drop(conn);

    // Merge should remove the file from the local branch.
    JointDirectory::new(Some(branches[0].clone()), [local_root.clone(), remote_root])
        .merge(&pool)
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    local_root.refresh(&mut conn).await.unwrap();

    assert_eq!(local_root.entries().count(), 1);

    let entry = local_root.entries().next().unwrap();

    assert_eq!(entry.name(), "dog.jpg");
    assert!(entry.is_tombstone());
}

// TODO: merge directory with missing blocks

#[tokio::test(flavor = "multi_thread")]
async fn remove_non_empty_subdirectory() {
    let (_base_dir, pool, branches) = setup(2).await;
    let mut tx = pool.begin().await.unwrap();

    let mut local_root = branches[0].open_or_create_root(&mut tx).await.unwrap();
    let mut local_dir = local_root
        .create_directory(&mut tx, "dir0".into())
        .await
        .unwrap();
    create_file(&mut tx, &mut local_dir, "foo.txt", &[], &branches[0]).await;

    local_root
        .create_directory(&mut tx, "dir1".into())
        .await
        .unwrap();

    let mut remote_root = branches[1].open_or_create_root(&mut tx).await.unwrap();
    let mut remote_dir = remote_root
        .create_directory(&mut tx, "dir0".into())
        .await
        .unwrap();
    create_file(&mut tx, &mut remote_dir, "bar.txt", &[], &branches[1]).await;

    remote_root
        .create_directory(&mut tx, "dir2".into())
        .await
        .unwrap();

    let mut root =
        JointDirectory::new(Some(branches[0].clone()), [local_root.clone(), remote_root]);

    root.remove_entry_recursively(&mut tx, "dir0")
        .await
        .unwrap();

    assert_matches!(root.lookup("dir0").next(), None);
    assert!(root.lookup("dir1").next().is_some());
    assert!(root.lookup("dir2").next().is_some());

    local_root.refresh(&mut tx).await.unwrap();
    assert_matches!(local_root.lookup("dir0"), Ok(EntryRef::Tombstone(_)));
    assert_matches!(local_root.lookup("dir1"), Ok(EntryRef::Directory(_)));
}

async fn setup(branch_count: usize) -> (TempDir, db::Pool, Vec<Branch>) {
    setup_with_rng(StdRng::from_entropy(), branch_count).await
}

// Useful for debugging non-deterministic failures.
async fn setup_with_rng(mut rng: StdRng, branch_count: usize) -> (TempDir, db::Pool, Vec<Branch>) {
    let (base_dir, pool) = db::create_temp().await.unwrap();
    let (event_tx, _) = broadcast::channel(1);
    let secrets = WriteSecrets::generate(&mut rng);
    let blob_cache = Arc::new(BlobCache::new(event_tx.clone()));

    let branches = {
        let pool = &pool;
        future::join_all((0..branch_count).map(|_| {
            let id = PublicKey::generate(&mut rng);
            let event_tx = event_tx.clone();
            let secrets = secrets.clone();
            let blob_cache = blob_cache.clone();

            async move {
                let data = BranchData::create(
                    &mut pool.acquire().await.unwrap(),
                    id,
                    &secrets.write_keys,
                    event_tx,
                )
                .await
                .unwrap();
                Branch::new(Arc::new(data), secrets.into(), blob_cache)
            }
        }))
        .await
    };

    (base_dir, pool, branches)
}

fn assert_unique_and_ordered<'a, I>(count: usize, mut entries: I)
where
    I: Iterator<Item = JointEntryRef<'a>>,
{
    let prev = entries.next();

    if prev.is_none() {
        assert!(count == 0);
        return;
    }

    let mut prev = prev.unwrap();
    let mut prev_i = 1;

    for entry in entries {
        assert!(prev.unique_name() < entry.unique_name());
        prev_i += 1;
        prev = entry;
    }

    assert_eq!(prev_i, count);
}

async fn create_file(
    conn: &mut db::Connection,
    parent: &mut Directory,
    name: &str,
    content: &[u8],
    local_branch: &Branch,
) -> File {
    let mut file = parent.create_file(conn, name.to_owned()).await.unwrap();
    let mut tx = conn.begin().await.unwrap();

    if !content.is_empty() {
        file.fork(&mut tx, local_branch.clone()).await.unwrap();
        file.write(&mut tx, content).await.unwrap();
    }

    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    file
}

async fn update_file(
    conn: &mut db::Connection,
    parent: &Directory,
    name: &str,
    content: &[u8],
    local_branch: &Branch,
) {
    let mut file = open_file(conn, parent, name).await;

    file.fork(conn, local_branch.clone()).await.unwrap();

    let mut tx = conn.begin().await.unwrap();
    file.truncate(&mut tx, 0).await.unwrap();
    file.write(&mut tx, content).await.unwrap();
    file.flush(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
}

async fn open_file(conn: &mut db::Connection, parent: &Directory, name: &str) -> File {
    parent
        .lookup(name)
        .unwrap()
        .file()
        .unwrap()
        .open(conn)
        .await
        .unwrap()
}

async fn read_version_vector(parent: &Directory, name: &str) -> VersionVector {
    parent
        .lookup(name)
        .unwrap()
        .file()
        .unwrap()
        .version_vector()
        .clone()
}
