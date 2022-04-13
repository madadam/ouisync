//! Garbage collection tests

use ouisync::{File, Repository, BLOB_HEADER_SIZE, BLOCK_SIZE};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Duration;
use tokio::time;

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn local_delete_local_file() {
    let mut rng = StdRng::seed_from_u64(0);
    let repo = common::create_repo(&mut rng).await;

    assert_eq!(repo.count_blocks().await.unwrap(), 0);

    repo.create_file("test.dat")
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    // 1 block for the file + 1 block for the root directory
    assert_eq!(repo.count_blocks().await.unwrap(), 2);

    repo.remove_entry("test.dat").await.unwrap();

    // just 1 block for the root directory
    assert_eq!(repo.count_blocks().await.unwrap(), 1);
}

// FIXME: currently when removing remote a file, we only put a tombstone into the local version of
// the parent directory but we don't delete the blocks of the file.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn local_delete_remote_file() {
    let mut rng = StdRng::seed_from_u64(0);

    let (network_l, network_r) = common::create_connected_peers().await;
    let (repo_l, repo_r) = common::create_linked_repos(&mut rng).await;
    let _reg_l = network_l.handle().register(repo_l.index().clone()).await;
    let _reg_r = network_r.handle().register(repo_r.index().clone()).await;

    repo_r
        .create_file("test.dat")
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    // 1 block for the file + 1 block for the remote root directory
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 2))
        .await
        .unwrap();

    repo_l.remove_entry("test.dat").await.unwrap();

    // Both the remote file and the remote root directory are removed.
    assert_eq!(repo_l.count_blocks().await.unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_delete_remote_file() {
    let mut rng = StdRng::seed_from_u64(0);

    let (network_l, network_r) = common::create_connected_peers().await;
    let (repo_l, repo_r) = common::create_linked_repos(&mut rng).await;
    let _reg_l = network_l.handle().register(repo_l.index().clone()).await;
    let _reg_r = network_r.handle().register(repo_r.index().clone()).await;

    repo_r
        .create_file("test.dat")
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    // 1 block for the file + 1 block for the remote root directory
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 2))
        .await
        .unwrap();

    repo_r.remove_entry("test.dat").await.unwrap();

    // The remote file is removed but the remote root remains to track the tombstone
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_truncate_local_file() {
    let mut rng = StdRng::seed_from_u64(0);
    let repo = common::create_repo(&mut rng).await;

    let mut file = repo.create_file("test.dat").await.unwrap();
    write_to_file(&mut rng, &mut file, 2 * BLOCK_SIZE - BLOB_HEADER_SIZE).await;
    file.flush().await.unwrap();

    // 2 blocks for the file + 1 block for the root directory
    assert_eq!(repo.count_blocks().await.unwrap(), 3);

    file.truncate(0).await.unwrap();
    file.flush().await.unwrap();

    // 1 block for the file + 1 block for the root directory
    assert_eq!(repo.count_blocks().await.unwrap(), 2);
}

// FIXME: `truncate(0)` currently removes the leaf nodes pointing to the original blocks and
// replaces them with one new leaf node pointing to the new block. The original blocks are still
// referenced by the remote branch and are not removed.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn local_truncate_remote_file() {
    let mut rng = StdRng::seed_from_u64(0);

    let (network_l, network_r) = common::create_connected_peers().await;
    let (repo_l, repo_r) = common::create_linked_repos(&mut rng).await;
    let _reg_l = network_l.handle().register(repo_l.index().clone()).await;
    let _reg_r = network_r.handle().register(repo_r.index().clone()).await;

    let mut file = repo_r.create_file("test.dat").await.unwrap();
    write_to_file(&mut rng, &mut file, 2 * BLOCK_SIZE - BLOB_HEADER_SIZE).await;
    file.flush().await.unwrap();

    // 2 blocks for the file + 1 block for the remote root directory
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 3))
        .await
        .unwrap();

    let mut file = repo_l.open_file("test.dat").await.unwrap();
    file.fork(&repo_l.get_or_create_local_branch().await.unwrap())
        .await
        .unwrap();
    file.truncate(0).await.unwrap();
    file.flush().await.unwrap();

    //   1 block for the file (the original 2 blocks were removed)
    // + 1 block for the local root (created when the file was forked)
    // + 0 blocks for the remote root (removed for being outdated)
    assert_eq!(repo_l.count_blocks().await.unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_truncate_remote_file() {
    let mut rng = StdRng::seed_from_u64(0);

    let (network_l, network_r) = common::create_connected_peers().await;
    let (repo_l, repo_r) = common::create_linked_repos(&mut rng).await;
    let _reg_l = network_l.handle().register(repo_l.index().clone()).await;
    let _reg_r = network_r.handle().register(repo_r.index().clone()).await;

    let mut file = repo_r.create_file("test.dat").await.unwrap();
    write_to_file(&mut rng, &mut file, 2 * BLOCK_SIZE - BLOB_HEADER_SIZE).await;
    file.flush().await.unwrap();

    // 2 blocks for the file + 1 block for the remote root
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 3))
        .await
        .unwrap();

    file.truncate(0).await.unwrap();
    file.flush().await.unwrap();

    // 1 block for the file + 1 block for the remote root
    time::timeout(Duration::from_secs(5), expect_block_count(&repo_l, 2))
        .await
        .unwrap();
}

async fn expect_block_count(repo: &Repository, expected_count: usize) {
    common::eventually(repo, || async {
        let actual_count = repo.count_blocks().await.unwrap();
        actual_count == expected_count
    })
    .await
}

async fn write_to_file(rng: &mut StdRng, file: &mut File, size: usize) {
    let mut buffer = vec![0; size];
    rng.fill(&mut buffer[..]);
    file.write(&buffer).await.unwrap();
}
