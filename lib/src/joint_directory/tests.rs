use super::*;
use crate::{
    access_control::WriteSecrets,
    branch::{Branch, BranchShared},
    crypto::sign::PublicKey,
    db,
    directory::{DirectoryFallback, DirectoryLocking},
    event::EventSender,
    store::Store,
    version_vector::VersionVector,
};
use assert_matches::assert_matches;
use rand::{rngs::StdRng, SeedableRng};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn no_conflict() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();
    create_file(&mut root0, "file0.txt", &[]).await;

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    create_file(&mut root1, "file1.txt", &[]).await;

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1]);
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
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();
    create_file(&mut root0, "file.txt", &[]).await;

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    create_file(&mut root1, "file.txt", &[]).await;

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file0 = entries
        .iter()
        .find(|file| file.first_branch().id() == branch0.id())
        .unwrap();

    let unique_name = conflict::create_unique_name("file.txt", branch0.id());

    assert_eq!(file0.name(), "file.txt");
    assert_eq!(&file0.unique_name(), &unique_name);

    let file_ref = root.lookup_unique(&unique_name).unwrap().file().unwrap();
    assert_eq!(file_ref.name(), file0.name());
    assert_eq!(file_ref.branch().id(), branch0.id());

    let file1 = entries
        .iter()
        .find(|file| file.first_branch().id() == branch1.id())
        .unwrap();

    let unique_name = conflict::create_unique_name("file.txt", branch1.id());

    assert_eq!(file1.name(), "file.txt");
    assert_eq!(&file1.unique_name(), &unique_name);

    let file_ref = root.lookup_unique(&unique_name).unwrap().file().unwrap();
    assert_eq!(file_ref.name(), file1.name());
    assert_eq!(file_ref.branch().id(), branch1.id());

    let entries: Vec<_> = root.lookup("file.txt").collect();
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .any(|entry| entry.first_branch().id() == branch0.id()),);
    assert!(entries
        .iter()
        .any(|entry| entry.first_branch().id() == branch1.id()));

    assert_matches!(root.lookup_unique("file.txt"), Err(Error::AmbiguousEntry));

    assert_unique_and_ordered(2, root.entries());
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_forked_files() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();
    create_file(&mut root0, "file.txt", b"one").await;

    // Fork the file into branch 1 and then modify it.
    let mut file1 = open_file(&root0, "file.txt").await;

    file1.fork(branch1.clone()).await.unwrap();
    file1.write_all(b"two").await.unwrap();
    file1.flush().await.unwrap();
    drop(file1);

    // Modify the file by branch 0 as well, to create concurrent versions
    let mut file0 = open_file(&root0, "file.txt").await;
    file0.write_all(b"three").await.unwrap();
    file0.flush().await.unwrap();

    // Refresh branch 0's root to reflect the changes
    root0.refresh().await.unwrap();

    // Open branch 1's root dir which should have been created in the process.
    let root1 = branch1
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    let root = JointDirectory::new(Some(branch1.clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    for branch in [&branch0, &branch1] {
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
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();
    root0.create_directory("dir".to_owned()).await.unwrap();

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    root1.create_directory("dir".to_owned()).await.unwrap();

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1]);

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
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();

    create_file(&mut root0, "config", &[]).await;

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    root1.create_directory("config".to_owned()).await.unwrap();

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::File(_)))
        .unwrap();

    assert_eq!(file_entry.name(), "config");
    assert_eq!(
        &file_entry.unique_name(),
        &conflict::create_unique_name("config", branch0.id())
    );

    let dir_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::Directory(_)))
        .unwrap();

    assert_eq!(dir_entry.name(), "config");
    assert_eq!(
        &dir_entry.unique_name(),
        &conflict::create_unique_name("config", branch1.id())
    );

    let entries = root.lookup("config");
    assert_eq!(entries.count(), 2);

    assert_matches!(root.lookup_unique("config"), Err(Error::AmbiguousEntry));

    let name = conflict::create_unique_name("config", branch0.id());
    let entry = root.lookup_unique(&name).unwrap();
    assert_eq!(entry.entry_type(), EntryType::File);
    assert_eq!(entry.file().unwrap().branch().id(), branch0.id());

    let name = conflict::create_unique_name("config", branch1.id());
    let entry = root.lookup_unique(&name).unwrap();
    assert_eq!(entry.entry_type(), EntryType::Directory);
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_file_and_multi_version_directory() {
    let (_base_dir, mut branches): (_, [_; 3]) = setup().await;

    // Sort the branches by their ids because directory disambiguator is the lexicographically
    // first branch id.
    branches.sort_by(|lhs, rhs| lhs.id().cmp(rhs.id()));
    let [branch0, branch1, branch2] = branches;

    let mut root0 = branch0.open_or_create_root().await.unwrap();
    create_file(&mut root0, "config", &[]).await;

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    root1.create_directory("config".to_owned()).await.unwrap();

    let mut root2 = branch2.open_or_create_root().await.unwrap();
    root2.create_directory("config".to_owned()).await.unwrap();

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1, root2]);

    let entries: Vec<_> = root.entries().collect();
    assert_eq!(entries.len(), 2);

    let file_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::File(_)))
        .unwrap();

    assert_eq!(
        &file_entry.unique_name(),
        &conflict::create_unique_name("config", branch0.id())
    );

    let dir_entry = entries
        .iter()
        .find(|entry| matches!(entry, JointEntryRef::Directory(_)))
        .unwrap();

    assert_eq!(
        &dir_entry.unique_name(),
        &conflict::create_unique_name("config", branch1.id())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_identical_versions() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    // Create a file by one branch.
    let mut root0 = branch0.open_or_create_root().await.unwrap();
    create_file(&mut root0, "file.txt", b"one").await;

    // Fork it into the other branch, creating an identical version of it.
    let mut file1 = open_file(&root0, "file.txt").await;
    file1.fork(branch1.clone()).await.unwrap();

    let root1 = branch1
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();

    // Create joint directory using branch 1 as the local branch.
    let root = JointDirectory::new(Some(branch1.clone()), [root0, root1]);

    // The file appears among the entries only once...
    assert_eq!(root.entries().count(), 1);

    // ...and it is the local version.
    let file = root
        .entries()
        .next()
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert_eq!(file.branch().id(), branch1.id());
    drop(file);

    // The file can also be retreived using `lookup`...
    let mut versions = root.lookup("file.txt");
    assert!(versions.next().is_some());
    assert!(versions.next().is_none());

    // ...and `lookup_version` using the author branch:
    root.lookup_version("file.txt", branch0.id()).unwrap();
}

//// TODO: test conflict_forked_directories
//// TODO: test conflict_multiple_files_and_directories
//// TODO: test conflict_file_with_name_containing_branch_prefix

#[tokio::test(flavor = "multi_thread")]
async fn cd_into_concurrent_directory() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut root0 = branch0.open_or_create_root().await.unwrap();

    let mut dir0 = root0.create_directory("pics".to_owned()).await.unwrap();
    create_file(&mut dir0, "dog.jpg", &[]).await;

    let mut root1 = branch1.open_or_create_root().await.unwrap();
    let mut dir1 = root1.create_directory("pics".to_owned()).await.unwrap();
    create_file(&mut dir1, "cat.jpg", &[]).await;

    let root = JointDirectory::new(Some(branch0.clone()), [root0, root1]);
    let dir = root.cd("pics").await.unwrap();

    let entries: Vec<_> = dir.entries().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name(), "cat.jpg");
    assert_eq!(entries[1].name(), "dog.jpg");
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_non_existing_file() {
    // 0 - local, 1 - remote
    let (_base_dir, [branch0, branch1]) = setup().await;

    let content = b"cat";

    // Create local root dir
    let mut local_root = branch0.open_or_create_root().await.unwrap();

    // Create remote root dir
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // Create a file in the remote root
    create_file(&mut remote_root, "cat.jpg", content).await;

    // Construct a joint directory over both root dirs and merge it.
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    local_root.refresh().await.unwrap();

    // Verify the file now exists in the local branch.
    let local_content = open_file(&local_root, "cat.jpg")
        .await
        .read_to_end()
        .await
        .unwrap();
    assert_eq!(local_content, content);

    // Local branch is up to date
    assert!(
        local_root.version_vector().await.unwrap() >= remote_root.version_vector().await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_older_file() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let content_v0 = b"version 0";
    let content_v1 = b"version 1";

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // Create a file in the remote root
    create_file(&mut remote_root, "cat.jpg", content_v0).await;

    // Merge to transfer the file to the local branch
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    // Modify the file by the remote branch
    update_file(&remote_root, "cat.jpg", content_v1, &branch1).await;

    local_root.refresh().await.unwrap();
    remote_root.refresh().await.unwrap();

    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    local_root.refresh().await.unwrap();

    let local_content = open_file(&local_root, "cat.jpg")
        .await
        .read_to_end()
        .await
        .unwrap();
    assert_eq!(local_content, content_v1);

    // Local branch is up to date
    let local_vv = local_root.version_vector().await.unwrap();
    let remote_vv = remote_root.version_vector().await.unwrap();

    assert!(local_vv >= remote_vv, "{local_vv:?} >= {remote_vv:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_locally_newer_file() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let content_v0 = b"version 0";
    let content_v1 = b"version 1";

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    create_file(&mut remote_root, "cat.jpg", content_v0).await;

    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    // Modify the file by the local branch
    local_root.refresh().await.unwrap();
    update_file(&local_root, "cat.jpg", content_v1, &branch0).await;

    JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root])
        .merge()
        .await
        .unwrap();

    let local_content = local_root
        .lookup("cat.jpg")
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap()
        .read_to_end()
        .await
        .unwrap();
    assert_eq!(local_content, content_v1);
}

mod attempt_to_merge_concurrent_file {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn in_root() {
        let (_base_dir, [local_branch, remote_branch]) = setup().await;

        let local_dir = local_branch.open_or_create_root().await.unwrap();
        let remote_dir = remote_branch.open_or_create_root().await.unwrap();

        case(local_dir, remote_dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_subdirectory() {
        let (_base_dir, [local_branch, remote_branch]) = setup().await;

        let dir_name = "dir";
        let local_dir = local_branch
            .open_or_create_root()
            .await
            .unwrap()
            .create_directory(dir_name.into())
            .await
            .unwrap();
        let remote_dir = remote_branch
            .open_or_create_root()
            .await
            .unwrap()
            .create_directory(dir_name.into())
            .await
            .unwrap();

        case(local_dir, remote_dir).await;
    }

    async fn case(mut local_dir: Directory, mut remote_dir: Directory) {
        let local_branch = local_dir.branch().clone();
        let remote_branch = remote_dir.branch().clone();

        create_file(&mut remote_dir, "cat.jpg", b"v0").await;

        merge(local_branch.clone(), remote_branch.clone())
            .await
            .unwrap();

        local_dir.refresh().await.unwrap();
        remote_dir.refresh().await.unwrap();

        // Modify the file by both branches concurrently
        update_file(&local_dir, "cat.jpg", b"v1", &local_branch).await;
        update_file(&remote_dir, "cat.jpg", b"v2", &remote_branch).await;

        assert_matches!(
            merge(local_branch.clone(), remote_branch.clone()).await,
            Err(Error::AmbiguousEntry)
        );

        local_dir.refresh().await.unwrap();

        // The local version is unchanged
        let content = open_file(&local_dir, "cat.jpg")
            .await
            .read_to_end()
            .await
            .unwrap();
        assert_eq!(content, b"v1");

        // The branches are still concurrent
        let local_vv = local_branch.version_vector().await.unwrap();
        let remote_vv = remote_branch.version_vector().await.unwrap();
        assert_eq!(local_vv.partial_cmp(&remote_vv), None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_is_idempotent() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let local_root = branch0.open_or_create_root().await.unwrap();
    let vv0 = branch0.version_vector().await.unwrap();

    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // Merge after a remote modification - this causes local modification.
    create_file(&mut remote_root, "cat.jpg", b"v0").await;

    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let vv1 = branch0.version_vector().await.unwrap();
    assert!(vv1 > vv0);

    // Merge again. This time there is no local modification because there was no remote
    // modification either.
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let vv2 = branch0.version_vector().await.unwrap();
    assert_eq!(vv2, vv1);

    // Perform another remote modification and merge again - this causes local modification
    // again.
    update_file(&remote_root, "cat.jpg", b"v1", &branch1).await;

    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let vv3 = branch0.version_vector().await.unwrap();
    assert!(vv3 > vv2);

    // Another idempotent merge which causes no local modification.
    JointDirectory::new(Some(branch0.clone()), [local_root, remote_root])
        .merge()
        .await
        .unwrap();

    let vv4 = branch0.version_vector().await.unwrap();
    assert_eq!(vv4, vv3);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_create_file_roundtrip() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let local_root = branch0.open_or_create_root().await.unwrap();
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // remote: create the file
    create_file(&mut remote_root, "cat.jpg", b"v0").await;

    // local: merge from remote
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let local_vv0 = branch0.version_vector().await.unwrap();

    // remote: merge from local
    JointDirectory::new(
        Some(branch1.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let remote_vv0 = branch1.version_vector().await.unwrap();

    assert_eq!(local_vv0, remote_vv0);

    // local: merge from remote - this has no effect
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let local_vv1 = branch0.version_vector().await.unwrap();
    assert_eq!(local_vv1, local_vv0);

    // remote: merge from local - this has no effect either
    JointDirectory::new(Some(branch1.clone()), [local_root, remote_root])
        .merge()
        .await
        .unwrap();

    let remote_vv1 = branch1.version_vector().await.unwrap();
    assert_eq!(remote_vv1, remote_vv0);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_create_and_delete_file_roundtrip() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // local: create the file
    create_file(&mut local_root, "monkey.jpg", b"v0").await;

    // remote: merge from local
    JointDirectory::new(
        Some(branch1.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    remote_root.refresh().await.unwrap();

    // remote: remove the file
    let file_vv = remote_root
        .lookup("monkey.jpg")
        .unwrap()
        .version_vector()
        .clone();
    remote_root
        .remove_entry(
            "monkey.jpg",
            branch1.id(),
            EntryTombstoneData::removed(file_vv),
        )
        .await
        .unwrap();

    // local: merge from remote
    JointDirectory::new(
        Some(branch0.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let local_vv = branch0.version_vector().await.unwrap();

    // remote: merge from local
    JointDirectory::new(
        Some(branch1.clone()),
        [local_root.clone(), remote_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    let remote_vv = branch1.version_vector().await.unwrap();

    assert_eq!(local_vv, remote_vv);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_remote_only() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut remote_root = branch1.open_or_create_root().await.unwrap();
    create_file(&mut remote_root, "cat.jpg", b"v0").await;

    // When passing only the remote dir to the joint directory the merge still works.
    JointDirectory::new(Some(branch0.clone()), [remote_root])
        .merge()
        .await
        .unwrap();

    let local_root = branch0
        .open_root(DirectoryLocking::Enabled, DirectoryFallback::Disabled)
        .await
        .unwrap();
    local_root.lookup("cat.jpg").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_sequential_modifications() {
    let (_base_dir, [branch0, branch1]) = setup_with_rng(StdRng::seed_from_u64(0)).await;

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut remote_root = branch1.open_or_create_root().await.unwrap();

    // Create a file by local, then modify it by remote, then read it back by local verifying
    // the modification by remote got through.

    create_file(&mut local_root, "dog.jpg", b"v0").await;
    local_root.refresh().await.unwrap();

    let vv0 = read_version_vector(&local_root, "dog.jpg").await;

    JointDirectory::new(
        Some(branch1.clone()),
        [remote_root.clone(), local_root.clone()],
    )
    .merge()
    .await
    .unwrap();

    remote_root.refresh().await.unwrap();

    let vv1 = read_version_vector(&remote_root, "dog.jpg").await;
    assert_eq!(vv1, vv0);

    update_file(&remote_root, "dog.jpg", b"v1", &branch1).await;
    remote_root.refresh().await.unwrap();

    let vv2 = read_version_vector(&remote_root, "dog.jpg").await;
    assert!(vv2 > vv1);

    JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root])
        .merge()
        .await
        .unwrap();

    local_root.refresh().await.unwrap();

    let entry = local_root.lookup("dog.jpg").unwrap().file().unwrap();
    assert_eq!(entry.version_vector(), &vv2);

    let content = entry.open().await.unwrap().read_to_end().await.unwrap();
    assert_eq!(content, b"v1");
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_concurrent_directories() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut local_dir = local_root.create_directory("dir".into()).await.unwrap();
    create_file(&mut local_dir, "dog.jpg", &[]).await;

    local_dir.refresh().await.unwrap();
    let local_dir_vv = local_dir.version_vector().await.unwrap();

    let mut remote_root = branch1.open_or_create_root().await.unwrap();
    let mut remote_dir = remote_root.create_directory("dir".into()).await.unwrap();
    create_file(&mut remote_dir, "cat.jpg", &[]).await;

    remote_dir.refresh().await.unwrap();
    let remote_dir_vv = remote_dir.version_vector().await.unwrap();

    JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root])
        .merge()
        .await
        .unwrap();

    local_root.refresh().await.unwrap();

    assert_eq!(local_root.entries().count(), 1);

    let entry = local_root.entries().next().unwrap();
    assert_eq!(entry.name(), "dir");
    assert_matches!(entry, EntryRef::Directory(_));

    // version vectors are merged
    assert_eq!(entry.version_vector(), &local_dir_vv.merged(&remote_dir_vv));

    let dir = entry
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();
    assert_eq!(dir.entries().count(), 2);

    dir.lookup("dog.jpg").unwrap().file().unwrap();
    dir.lookup("cat.jpg").unwrap().file().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_file_and_tombstone() {
    // Create two branches.
    let (_base_dir, [branch0, branch1]) = setup().await;

    // Create a file in the local one.
    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut file = create_file(&mut local_root, "dog.jpg", &[]).await;
    let file_vv = file.version_vector().await.unwrap();

    // Fork the file into the remote branch.
    let mut remote_root = branch1.open_or_create_root().await.unwrap();
    file.fork(branch1.clone()).await.unwrap();
    drop(file);

    // Remove the file in the remote branch.
    remote_root
        .remove_entry(
            "dog.jpg",
            branch1.id(),
            EntryTombstoneData::removed(file_vv),
        )
        .await
        .unwrap();

    // Merge should remove the file from the local branch.
    JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root])
        .merge()
        .await
        .unwrap();

    local_root.refresh().await.unwrap();

    assert_eq!(local_root.entries().count(), 1);

    let entry = local_root.entries().next().unwrap();

    assert_eq!(entry.name(), "dog.jpg");
    assert!(entry.is_tombstone());
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_moved_file() {
    // Create two branches.
    let (_base_dir, [branch0, branch1]) = setup().await;

    let dir_name = "b";
    let file_name = "a";
    let file_content = b"content of the file";

    // Create a file in the local one.
    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut file = create_file(&mut local_root, file_name, file_content).await;

    // Fork the file into the remote branch
    let mut remote_root = branch1.open_or_create_root().await.unwrap();
    file.fork(branch1.clone()).await.unwrap();

    // Drop the file otherwise moving it would be blocked (https://github.com/equalitie/ouisync/issues/58)
    drop(file);

    remote_root.refresh().await.unwrap();

    // Create a new directory in the remote branch
    let mut dir = remote_root
        .create_directory(dir_name.to_owned())
        .await
        .unwrap();

    // Move the file into the new directory
    let entry_data = remote_root.lookup(file_name).unwrap().clone_data();
    remote_root
        .move_entry(
            file_name,
            entry_data,
            &mut dir,
            file_name,
            VersionVector::first(*branch1.id()),
        )
        .await
        .unwrap();

    // Merge back into the local branch
    JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root])
        .merge()
        .await
        .unwrap();

    local_root.refresh().await.unwrap();

    // The file is moved from it's original location to the new directory
    assert_matches!(local_root.lookup(file_name), Ok(EntryRef::Tombstone(_)));

    let dir = local_root
        .lookup(dir_name)
        .unwrap()
        .directory()
        .unwrap()
        .open(DirectoryFallback::Disabled)
        .await
        .unwrap();
    let mut file = dir
        .lookup(file_name)
        .unwrap()
        .file()
        .unwrap()
        .open()
        .await
        .unwrap();

    assert_eq!(file.read_to_end().await.unwrap(), file_content);
}

// TODO: merge directory with missing blocks

#[tokio::test(flavor = "multi_thread")]
async fn remove_non_empty_subdirectory() {
    let (_base_dir, [branch0, branch1]) = setup().await;

    let mut local_root = branch0.open_or_create_root().await.unwrap();
    let mut local_dir = local_root.create_directory("dir0".into()).await.unwrap();
    create_file(&mut local_dir, "foo.txt", &[]).await;
    drop(local_dir);

    local_root.create_directory("dir1".into()).await.unwrap();

    let mut remote_root = branch1.open_or_create_root().await.unwrap();
    let mut remote_dir = remote_root.create_directory("dir0".into()).await.unwrap();
    create_file(&mut remote_dir, "bar.txt", &[]).await;
    drop(remote_dir);

    remote_root.create_directory("dir2".into()).await.unwrap();

    let mut root = JointDirectory::new(Some(branch0.clone()), [local_root.clone(), remote_root]);

    root.remove_entry_recursively("dir0").await.unwrap();

    assert_matches!(root.lookup("dir0").next(), None);
    assert!(root.lookup("dir1").next().is_some());
    assert!(root.lookup("dir2").next().is_some());

    local_root.refresh().await.unwrap();
    assert_matches!(local_root.lookup("dir0"), Ok(EntryRef::Tombstone(_)));
    assert_matches!(local_root.lookup("dir1"), Ok(EntryRef::Directory(_)));
}

async fn setup<const N: usize>() -> (TempDir, [Branch; N]) {
    setup_with_rng::<N>(StdRng::from_entropy()).await
}

// Useful for debugging non-deterministic failures.
async fn setup_with_rng<const N: usize>(mut rng: StdRng) -> (TempDir, [Branch; N]) {
    let (base_dir, pool) = db::create_temp().await.unwrap();
    let store = Store::new(pool);
    let event_tx = EventSender::new(1);
    let secrets = WriteSecrets::generate(&mut rng);
    let shared = BranchShared::new();

    let branches = [(); N].map(|_| {
        let id = PublicKey::generate(&mut rng);
        Branch::new(
            id,
            store.clone(),
            secrets.clone().into(),
            shared.clone(),
            event_tx.clone(),
        )
    });

    (base_dir, branches)
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

async fn create_file(parent: &mut Directory, name: &str, content: &[u8]) -> File {
    let mut file = parent.create_file(name.to_owned()).await.unwrap();

    if !content.is_empty() {
        file.write_all(content).await.unwrap();
    }

    file.flush().await.unwrap();

    file
}

async fn update_file(
    parent: &Directory,
    name: &str,
    content: &[u8],
    local_branch: &Branch,
) -> File {
    let mut file = open_file(parent, name).await;

    file.fork(local_branch.clone()).await.unwrap();

    file.truncate(0).unwrap();
    file.write_all(content).await.unwrap();
    file.flush().await.unwrap();

    file
}

async fn open_file(parent: &Directory, name: &str) -> File {
    parent
        .lookup(name)
        .unwrap()
        .file()
        .unwrap()
        .open()
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

async fn merge(local_branch: Branch, remote_branch: Branch) -> Result<()> {
    let local_root = local_branch.open_or_create_root().await.unwrap();
    let remote_root = remote_branch.open_or_create_root().await.unwrap();

    JointDirectory::new(Some(local_branch.clone()), [local_root, remote_root])
        .merge()
        .await?;

    Ok(())
}
