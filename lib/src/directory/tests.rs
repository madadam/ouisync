use super::*;
use crate::{
    access_control::{AccessKeys, WriteSecrets},
    branch::BranchShared,
    db,
    event::EventSender,
    store::{self, Store},
    test_utils,
};
use assert_matches::assert_matches;
use std::collections::BTreeSet;
use tempfile::TempDir;
use tracing::Instrument;

#[tokio::test(flavor = "multi_thread")]
async fn create_and_list_entries() {
    let (_base_dir, branch) = setup().await;

    // Create the root directory and put some file in it.
    let mut dir = branch.open_or_create_root().await.unwrap();

    let mut file_dog = dir.create_file("dog.txt".into()).await.unwrap();
    file_dog.write_all(b"woof").await.unwrap();
    file_dog.flush().await.unwrap();

    let mut file_cat = dir.create_file("cat.txt".into()).await.unwrap();
    file_cat.write_all(b"meow").await.unwrap();
    file_cat.flush().await.unwrap();

    // Reopen the dir and try to read the files.
    let dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    let expected_names: BTreeSet<_> = ["dog.txt", "cat.txt"].into_iter().collect();
    let actual_names: BTreeSet<_> = dir.entries().map(|entry| entry.name()).collect();
    assert_eq!(actual_names, expected_names);

    for &(file_name, expected_content) in &[("dog.txt", b"woof"), ("cat.txt", b"meow")] {
        let mut file = dir
            .lookup(file_name)
            .unwrap()
            .file()
            .unwrap()
            .open()
            .await
            .unwrap();
        let actual_content = file.read_to_end().await.unwrap();
        assert_eq!(actual_content, expected_content);
    }
}

// TODO: test update existing directory
#[tokio::test(flavor = "multi_thread")]
async fn add_entry_to_existing_directory() {
    let (_base_dir, branch) = setup().await;

    // Create empty directory and add a file to it.
    let mut dir = branch.open_or_create_root().await.unwrap();
    dir.create_file("one.txt".into()).await.unwrap();

    // Reopen it and add another file to it.
    let mut dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    dir.create_file("two.txt".into()).await.unwrap();

    // Reopen it again and check boths files are still there.
    let dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    assert!(dir.lookup("one.txt").is_ok());
    assert!(dir.lookup("two.txt").is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_file() {
    let (_base_dir, branch) = setup().await;

    let name = "monkey.txt";

    // Create a directory with a single file.
    let mut parent_dir = branch.open_or_create_root().await.unwrap();
    let mut file = parent_dir.create_file(name.into()).await.unwrap();
    file.flush().await.unwrap();

    let file_vv = file.version_vector().await.unwrap();
    let file_id = *file.blob_id();
    drop(file);

    // Reopen and remove the file
    let mut parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    parent_dir
        .remove_entry(name, branch.id(), file_vv)
        .await
        .unwrap();

    // Reopen again and check the file entry was removed.
    let parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    assert_matches!(parent_dir.lookup(name), Ok(EntryRef::Tombstone(_)));
    assert_eq!(parent_dir.entries().count(), 1);

    // Check the file blob was removed as well
    assert_matches!(
        Blob::open(
            &mut branch.store().begin_read().await.unwrap(),
            branch.clone(),
            file_id,
        )
        .await,
        Err(Error::Store(store::Error::LocatorNotFound))
    );

    // Try re-creating the file again
    let mut parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    let mut file = parent_dir.create_file(name.into()).await.unwrap();
    file.flush().await.unwrap();
}

// Rename a file without moving it to another directory.
#[tokio::test(flavor = "multi_thread")]
async fn rename_file() {
    crate::test_utils::init_log();

    let (_base_dir, branch) = setup().await;

    let src_name = "zebra.txt";
    let dst_name = "donkey.txt";
    let content = b"hee-haw";

    // Create a directory with a single file.
    let mut parent_dir = branch.open_or_create_root().await.unwrap();
    let mut file = parent_dir.create_file(src_name.into()).await.unwrap();
    file.write_all(content).await.unwrap();
    file.flush().await.unwrap();

    let file_id = *file.blob_id();

    drop(file);

    // Reopen and move the file
    let mut parent_dir_src = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    let mut parent_dir_dst = parent_dir_src.clone();

    let entry_to_move = parent_dir_src.lookup(src_name).unwrap().clone_data();

    parent_dir_src
        .move_entry(
            src_name,
            entry_to_move,
            &mut parent_dir_dst,
            dst_name,
            VersionVector::first(*branch.id()),
        )
        .await
        .unwrap();

    // Reopen again and check the file entry was renamed.
    let parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    let mut dst_file = parent_dir
        .lookup(dst_name)
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();

    assert_eq!(&file_id, dst_file.blob_id());
    assert_eq!(&content[..], &dst_file.read_to_end().await.unwrap()[..]);

    let src_entry = parent_dir.lookup(src_name).unwrap();

    assert_matches!(src_entry, EntryRef::Tombstone(_));
}

#[tokio::test(flavor = "multi_thread")]
async fn move_file_within_branch() {
    let (_base_dir, branch) = setup().await;

    let file_name = "cow.txt";
    let content = b"moo";

    // Create a directory with a single file.
    let mut root_dir = branch.open_or_create_root().await.unwrap();
    let mut aux_dir = root_dir
        .create_directory("aux".into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    let mut file = root_dir.create_file(file_name.into()).await.unwrap();
    file.write_all(content).await.unwrap();
    file.flush().await.unwrap();
    root_dir.refresh().await.unwrap();

    let file_id = *file.blob_id();

    drop(file);

    //
    // Move the file from ./ to ./aux/
    //

    let entry_to_move = root_dir.lookup(file_name).unwrap().clone_data();

    root_dir
        .move_entry(
            file_name,
            entry_to_move,
            &mut aux_dir,
            file_name,
            VersionVector::first(*branch.id()),
        )
        .await
        .unwrap();

    let mut file = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup("aux")
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup(file_name)
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();

    assert_eq!(&file_id, file.blob_id());
    assert_eq!(&content[..], &file.read_to_end().await.unwrap()[..]);

    drop(file);

    //
    // Now move it back from ./aux/ to ./
    //

    let entry_to_move = aux_dir.lookup(file_name).unwrap().clone_data();

    let tombstone_vv = root_dir.lookup(file_name).unwrap().version_vector().clone();

    aux_dir
        .move_entry(
            file_name,
            entry_to_move,
            &mut root_dir,
            file_name,
            tombstone_vv.incremented(*branch.id()),
        )
        .await
        .unwrap();

    let mut file = root_dir
        .lookup(file_name)
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();

    assert_eq!(&content[..], &file.read_to_end().await.unwrap()[..]);
}

// Move directory "dir/" with content "cow.txt" to directory "dst/".
//
// Equivalent of:
//     $ mkdir dir
//     $ touch dir/cow.txt
//     $ mkdir dst
//     $ mv dir dst/
#[tokio::test(flavor = "multi_thread")]
async fn move_non_empty_directory() {
    let (_base_dir, branch) = setup().await;

    let dir_name = "dir";
    let dst_dir_name = "dst";
    let file_name = "cow.txt";
    let content = b"moo";

    // Create a directory with a single file.
    let mut root_dir = branch.open_or_create_root().await.unwrap();
    let mut dir = root_dir
        .create_directory(dir_name.into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    let mut file = dir.create_file(file_name.into()).await.unwrap();
    file.write_all(content).await.unwrap();
    file.flush().await.unwrap();
    let file_id = *file.blob_id();

    drop(file);
    drop(dir);

    let mut dst_dir = root_dir
        .create_directory(dst_dir_name.into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    let entry_to_move = root_dir.lookup(dir_name).unwrap().clone_data();

    root_dir
        .move_entry(
            dir_name,
            entry_to_move,
            &mut dst_dir,
            dir_name,
            VersionVector::first(*branch.id()),
        )
        .await
        .unwrap();

    let file = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup(dst_dir_name)
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup(dir_name)
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup(file_name)
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();

    assert_eq!(&file_id, file.blob_id());
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_subdirectory() {
    let (_base_dir, branch) = setup().await;

    let name = "dir";

    // Create a directory with a single subdirectory.
    let mut parent_dir = branch.open_or_create_root().await.unwrap();
    let dir = parent_dir
        .create_directory(name.into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();
    let dir_vv = dir.version_vector().await.unwrap();
    let dir_id = *dir.blob_id();
    drop(dir);

    // Reopen and remove the subdirectory
    let mut parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    parent_dir
        .remove_entry(name, branch.id(), dir_vv)
        .await
        .unwrap();

    // Reopen again and check the subdirectory entry was removed.
    let parent_dir = branch
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    assert_matches!(parent_dir.lookup(name), Ok(EntryRef::Tombstone(_)));

    // Check the subdirectory blob was removed as well
    assert_matches!(
        Blob::open(
            &mut branch.store().begin_read().await.unwrap(),
            branch.clone(),
            dir_id,
        )
        .await,
        Err(Error::Store(store::Error::LocatorNotFound))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fork_sanity_check() {
    test_utils::init_log();

    let (_base_dir, [branch0, branch1]) = setup_multiple().await;

    tracing::info!(branch0 = ?branch0.id(), branch1 = ?branch1.id());

    // Create a nested directory by branch 0
    let mut root0 = branch0.open_or_create_root().await.unwrap();
    let dir0 = root0
        .create_directory("dir".into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    // Fork it into branch 1
    let mut dir1 = dir0
        .fork(&branch1)
        .instrument(tracing::info_span!("fork"))
        .await
        .unwrap();
    assert_eq!(dir1.branch().id(), branch1.id());
    assert_eq!(dir1.blob_id(), dir0.blob_id());

    // Verify the root dir got forked as well
    let root1 = branch1
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    assert_eq!(root1.branch().id(), branch1.id());

    // Modify it
    dir1.create_file("dog.jpg".into()).await.unwrap();

    // Reopen orig dir and verify it's unchanged
    let dir = branch0
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup("dir")
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();

    assert_eq!(dir.entries().count(), 0);

    // Reopen forked dir and verify it contains the new file
    let dir = branch1
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap()
        .lookup("dir")
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();

    assert_eq!(
        dir.entries().map(|entry| entry.name()).next(),
        Some("dog.jpg")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fork_over_tombstone() {
    let (_base_dir, [branch0, branch1]) = setup_multiple().await;

    // Create a directory in branch 0 and delete it.
    let mut root0 = branch0.open_or_create_root().await.unwrap();
    root0
        .create_directory("dir".into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    let vv = root0.lookup("dir").unwrap().version_vector().clone();
    root0.remove_entry("dir", branch0.id(), vv).await.unwrap();

    // Create a directory with the same name in branch 1.
    let mut root1 = branch1.open_or_create_root().await.unwrap();
    root1
        .create_directory("dir".into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    // Open it by branch 0 and fork it.
    let root1_on_0 = branch1
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    let dir1 = root1_on_0
        .lookup("dir")
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();

    dir1.fork(&branch0).await.unwrap();

    // Check the forked dir now exists in branch 0.
    root0.refresh().await.unwrap();
    assert_matches!(root0.lookup("dir"), Ok(EntryRef::Directory(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn fork_over_existing_directory() {
    let (_base_dir, [branch0, branch1]) = setup_multiple().await;

    let name = "dir";

    // Create the directories with the same name but different blob id in each branch.
    let mut root0 = branch0.open_or_create_root().await.unwrap();
    let dir0 = root0
        .create_directory(name.into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    let _dir1 = root1
        .create_directory(name.into(), rand::random(), &VersionVector::new())
        .await
        .unwrap();

    // Fork back and forth. Afterwards both dirs should have the same blob id and vv.
    let dir1 = dir0.fork(&branch1).await.unwrap();
    let dir0 = dir1.fork(&branch0).await.unwrap();

    assert_eq!(dir0.blob_id(), dir1.blob_id());
    assert_eq!(
        dir0.version_vector().await.unwrap(),
        dir1.version_vector().await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn modify_directory_concurrently() {
    let (_base_dir, branch) = setup().await;
    let mut root = branch.open_or_create_root().await.unwrap();

    // Obtain two instances of the same directory, create a new file in one of them and verify
    // the file also exists in the other after refresh.

    let mut dir0 = root
        .create_directory("dir".to_owned(), rand::random(), &VersionVector::new())
        .await
        .unwrap();
    let mut dir1 = root
        .lookup("dir")
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();

    let mut file0 = dir0.create_file("file.txt".to_owned()).await.unwrap();
    file0.write_all(b"hello").await.unwrap();
    file0.flush().await.unwrap();

    dir1.refresh().await.unwrap();
    let mut file1 = dir1
        .lookup("file.txt")
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert_eq!(file1.read_to_end().await.unwrap(), b"hello");
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_remote_only_entry() {
    let (_base_dir, local_branch) = setup().await;

    let mut root = local_branch.open_or_create_root().await.unwrap();

    let name = "foo.txt";

    let remote_id = PublicKey::random();
    let remote_vv = vv![remote_id => 1]; // pretend there is a remote file with this vv

    root.remove_entry(name, &remote_id, remote_vv.clone())
        .await
        .unwrap();

    let local_vv = assert_matches!(
        root.lookup(name),
        Ok(EntryRef::Tombstone(entry)) => entry.version_vector()
    );

    assert!(*local_vv > remote_vv);
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_concurrent_entry() {
    let (_base_dir, local_branch) = setup().await;
    let mut root = local_branch.open_or_create_root().await.unwrap();

    let name = "foo.txt";
    root.create_file(name.to_owned()).await.unwrap();

    let remote_id = PublicKey::random();
    let remote_vv = vv![remote_id => 1]; // pretend there is a remote file with this vv

    root.remove_entry(name, &remote_id, remote_vv.clone())
        .await
        .unwrap();

    let local_vv = assert_matches!(
        root.lookup(name),
        Ok(EntryRef::File(entry)) => entry.version_vector()
    );

    assert!(*local_vv > remote_vv);
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_non_existing_local_entry() {
    let (_base_dir, local_branch) = setup().await;
    let mut root = local_branch.open_or_create_root().await.unwrap();
    let name = "foo.txt";

    let vv0 = local_branch.version_vector().await.unwrap();

    root.remove_entry(name, local_branch.id(), VersionVector::new())
        .await
        .unwrap();

    let vv1 = local_branch.version_vector().await.unwrap();
    assert!(vv1 > vv0);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_tombstone_over_non_existing_entry() {
    let (_base_dir, local_branch) = setup().await;
    let mut root = local_branch.open_or_create_root().await.unwrap();
    let name = "foo.txt";

    let vv0 = local_branch.version_vector().await.unwrap();

    let remote_id = PublicKey::random();
    root.create_tombstone(
        name,
        EntryTombstoneData::new(TombstoneCause::Removed, VersionVector::first(remote_id)),
    )
    .await
    .unwrap();

    let vv1 = local_branch.version_vector().await.unwrap();
    assert!(vv1 > vv0);
    assert_matches!(root.lookup(name), Ok(EntryRef::Tombstone(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_tombstone_over_existing_entry() {
    let (_base_dir, local_branch) = setup().await;
    let mut root = local_branch.open_or_create_root().await.unwrap();
    let name = "foo.txt";

    root.create_file(name.to_owned()).await.unwrap();
    let file_vv = root.lookup(name).unwrap().version_vector().clone();

    let vv0 = local_branch.version_vector().await.unwrap();

    let remote_id = PublicKey::random();

    root.create_tombstone(
        name,
        EntryTombstoneData::new(TombstoneCause::Removed, file_vv.incremented(remote_id)),
    )
    .await
    .unwrap();

    let vv1 = local_branch.version_vector().await.unwrap();
    assert!(vv1 > vv0);
    assert_matches!(root.lookup(name), Ok(EntryRef::Tombstone(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_tombstone_is_idempotent() {
    let (_base_dir, local_branch) = setup().await;
    let mut root = local_branch.open_or_create_root().await.unwrap();
    let name = "foo.txt";

    root.create_file(name.to_owned()).await.unwrap();

    let proof0 = local_branch.proof().await.unwrap();

    let remote_id = PublicKey::random();
    let tombstone_vv = root
        .lookup(name)
        .unwrap()
        .version_vector()
        .clone()
        .incremented(remote_id);

    root.create_tombstone(
        name,
        EntryTombstoneData::new(TombstoneCause::Removed, tombstone_vv.clone()),
    )
    .await
    .unwrap();

    let proof1 = local_branch.proof().await.unwrap();
    assert!(proof1.version_vector > proof0.version_vector);

    root.create_tombstone(
        name,
        EntryTombstoneData::new(TombstoneCause::Removed, tombstone_vv),
    )
    .await
    .unwrap();

    let proof2 = local_branch.proof().await.unwrap();
    assert_eq!(proof2, proof1);
}

async fn setup() -> (TempDir, Branch) {
    let (base_dir, [branch]) = setup_multiple().await;
    (base_dir, branch)
}

async fn setup_multiple<const N: usize>() -> (TempDir, [Branch; N]) {
    let (base_dir, pool) = db::create_temp().await.unwrap();
    let keys = AccessKeys::from(WriteSecrets::random());
    let branches = [(); N].map(|_| create_branch(pool.clone(), keys.clone()));

    (base_dir, branches)
}

fn create_branch(pool: db::Pool, keys: AccessKeys) -> Branch {
    let store = Store::new(pool);
    let id = PublicKey::random();
    let shared = BranchShared::new();
    let event_tx = EventSender::new(1);
    Branch::new(id, store, keys, shared, event_tx)
}
