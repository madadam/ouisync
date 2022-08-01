use super::*;
use crate::{
    access_control::WriteSecrets,
    block::{self, BLOCK_SIZE},
    crypto::sign::PublicKey,
    error::Error,
    index::BranchData,
    test_utils,
};
use assert_matches::assert_matches;
use proptest::collection::vec;
use rand::{distributions::Standard, prelude::*};
use std::sync::Arc;
use test_strategy::proptest;
use tokio::sync::broadcast;

#[tokio::test(flavor = "multi_thread")]
async fn empty_blob() {
    let (_, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
    blob.flush(&mut conn).await.unwrap();

    // Re-open the blob and read its contents.
    let mut blob = Blob::open(&mut conn, branch, Locator::ROOT, Shared::uninit())
        .await
        .unwrap();

    let mut buffer = [0; 1];
    assert_eq!(blob.read(&mut conn, &mut buffer[..]).await.unwrap(), 0);
}

#[proptest]
fn write_and_read(
    is_root: bool,
    #[strategy(1..3 * BLOCK_SIZE)] blob_len: usize,
    #[strategy(1..=#blob_len)] write_len: usize,
    #[strategy(1..=#blob_len + 1)] read_len: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(write_and_read_case(
        is_root, blob_len, write_len, read_len, rng_seed,
    ))
}

async fn write_and_read_case(
    is_root: bool,
    blob_len: usize,
    write_len: usize,
    read_len: usize,
    rng_seed: u64,
) {
    let (mut rng, pool, branch) = setup(rng_seed).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator = if is_root {
        Locator::ROOT
    } else {
        random_head_locator(&mut rng)
    };

    // Create the blob and write to it in chunks of `write_len` bytes.
    let mut blob = Blob::create(branch.clone(), locator, Shared::uninit());

    let orig_content: Vec<u8> = rng.sample_iter(Standard).take(blob_len).collect();

    for chunk in orig_content.chunks(write_len) {
        blob.write(&mut conn, chunk).await.unwrap();
    }

    blob.flush(&mut conn).await.unwrap();

    // Re-open the blob and read from it in chunks of `read_len` bytes
    let mut blob = Blob::open(&mut conn, branch.clone(), locator, Shared::uninit())
        .await
        .unwrap();

    let mut read_content = vec![0; 0];
    let mut read_buffer = vec![0; read_len];

    loop {
        let len = blob.read(&mut conn, &mut read_buffer[..]).await.unwrap();

        if len == 0 {
            break; // done
        }

        read_content.extend(&read_buffer[..len]);
    }

    assert_eq!(read_content.len(), orig_content.len());
    assert!(read_content == orig_content);
}

#[proptest]
fn len(
    #[strategy(0..3 * BLOCK_SIZE)] content_len: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(async {
        let (rng, pool, branch) = setup(rng_seed).await;
        let mut conn = pool.acquire().await.unwrap();

        let content: Vec<u8> = rng.sample_iter(Standard).take(content_len).collect();

        let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
        blob.write(&mut conn, &content[..]).await.unwrap();
        assert_eq!(blob.len().await, content_len as u64);

        blob.flush(&mut conn).await.unwrap();
        assert_eq!(blob.len().await, content_len as u64);

        let blob = Blob::open(&mut conn, branch, Locator::ROOT, Shared::uninit())
            .await
            .unwrap();
        assert_eq!(blob.len().await, content_len as u64);
    })
}

#[proptest]
fn seek_from_start(
    #[strategy(0..2 * BLOCK_SIZE)] content_len: usize,
    #[strategy(0..=#content_len)] pos: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(seek_from(
        content_len,
        SeekFrom::Start(pos as u64),
        pos,
        rng_seed,
    ))
}

#[proptest]
fn seek_from_end(
    #[strategy(0..2 * BLOCK_SIZE)] content_len: usize,
    #[strategy(0..=#content_len)] pos: usize,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(seek_from(
        content_len,
        SeekFrom::End(-((content_len - pos) as i64)),
        pos,
        rng_seed,
    ))
}

async fn seek_from(content_len: usize, seek_from: SeekFrom, expected_pos: usize, rng_seed: u64) {
    let (rng, pool, branch) = setup(rng_seed).await;
    let mut conn = pool.acquire().await.unwrap();

    let content: Vec<u8> = rng.sample_iter(Standard).take(content_len).collect();

    let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
    blob.write(&mut conn, &content[..]).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    blob.seek(&mut conn, seek_from).await.unwrap();

    let mut read_buffer = vec![0; content.len()];
    let len = blob.read(&mut conn, &mut read_buffer[..]).await.unwrap();
    assert_eq!(read_buffer[..len], content[expected_pos..]);
}

#[proptest]
fn seek_from_current(
    #[strategy(1..2 * BLOCK_SIZE)] content_len: usize,
    #[strategy(vec(0..#content_len, 1..10))] positions: Vec<usize>,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(async {
        let (rng, pool, branch) = setup(rng_seed).await;
        let mut conn = pool.acquire().await.unwrap();

        let content: Vec<u8> = rng.sample_iter(Standard).take(content_len).collect();

        let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
        blob.write(&mut conn, &content[..]).await.unwrap();
        blob.flush(&mut conn).await.unwrap();

        blob.seek(&mut conn, SeekFrom::Start(0)).await.unwrap();

        let mut prev_pos = 0;
        for pos in positions {
            blob.seek(&mut conn, SeekFrom::Current(pos as i64 - prev_pos as i64))
                .await
                .unwrap();
            prev_pos = pos;
        }

        let mut read_buffer = vec![0; content.len()];
        let len = blob.read(&mut conn, &mut read_buffer[..]).await.unwrap();
        assert_eq!(read_buffer[..len], content[prev_pos..]);
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn seek_after_end() {
    let (_, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let content = b"content";

    let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
    blob.write(&mut conn, &content[..]).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut read_buffer = [0];

    for &offset in &[0, 1, 2] {
        blob.seek(&mut conn, SeekFrom::Start(content.len() as u64 + offset))
            .await
            .unwrap();

        assert_eq!(blob.read(&mut conn, &mut read_buffer).await.unwrap(), 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn seek_before_start() {
    let (_, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let content = b"content";

    let mut blob = Blob::create(branch.clone(), Locator::ROOT, Shared::uninit());
    blob.write(&mut conn, &content[..]).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut read_buffer = vec![0; content.len()];

    for &offset in &[0, 1, 2] {
        blob.seek(&mut conn, SeekFrom::End(-(content.len() as i64) - offset))
            .await
            .unwrap();
        blob.read(&mut conn, &mut read_buffer).await.unwrap();
        assert_eq!(read_buffer, content);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_blob() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator0 = random_head_locator(&mut rng);
    let locator1 = locator0.next();

    let content: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .take(2 * BLOCK_SIZE)
        .collect();

    let mut blob = Blob::create(branch.clone(), locator0, Shared::uninit());
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let encoded_locator0 = locator0.encode(branch.keys().read());
    let encoded_locator1 = locator1.encode(branch.keys().read());

    let block_ids = {
        let id0 = branch
            .data()
            .get(&mut conn, &encoded_locator0)
            .await
            .unwrap();
        let id1 = branch
            .data()
            .get(&mut conn, &encoded_locator1)
            .await
            .unwrap();
        [id0, id1]
    };

    // Remove the blob
    Blob::remove(&mut conn, &branch, locator0).await.unwrap();
    trash_queue::flush_some(&mut conn, branch.keys(), u32::MAX)
        .await
        .unwrap();

    // Check the block entries were deleted from the index.
    assert_matches!(
        branch.data().get(&mut conn, &encoded_locator0).await,
        Err(Error::EntryNotFound)
    );
    assert_matches!(
        branch.data().get(&mut conn, &encoded_locator1).await,
        Err(Error::EntryNotFound)
    );

    // Check the blocks were deleted as well.
    for block_id in &block_ids {
        assert!(!block::exists(&mut conn, block_id).await.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_to_empty() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator0 = random_head_locator(&mut rng);
    let locator1 = locator0.next();

    let content: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .take(2 * BLOCK_SIZE)
        .collect();

    let mut blob = Blob::create(branch.clone(), locator0, Shared::uninit());
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let locator0 = locator0.encode(branch.keys().read());
    let locator1 = locator1.encode(branch.keys().read());

    let (old_block_id0, old_block_id1) = {
        let id0 = branch.data().get(&mut conn, &locator0).await.unwrap();
        let id1 = branch.data().get(&mut conn, &locator1).await.unwrap();

        (id0, id1)
    };

    blob.truncate(&mut conn, 0).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    // Check the blob is empty
    let mut buffer = [0; 1];
    blob.seek(&mut conn, SeekFrom::Start(0)).await.unwrap();
    assert_eq!(blob.read(&mut conn, &mut buffer).await.unwrap(), 0);
    assert_eq!(blob.len().await, 0);

    trash_queue::flush_some(&mut conn, branch.keys(), u32::MAX)
        .await
        .unwrap();

    // Check the second block entry was deleted from the index
    assert_matches!(
        branch.data().get(&mut conn, &locator1).await,
        Err(Error::EntryNotFound)
    );
    assert!(!block::exists(&mut conn, &old_block_id1).await.unwrap());

    // The first block is not deleted because it's needed to store the metadata.
    // It's only deleted when the blob itself is deleted.
    // Check that it was modified to store the new length though.
    let new_block_id0 = branch.data().get(&mut conn, &locator0).await.unwrap();
    assert_ne!(new_block_id0, old_block_id0);
    assert!(!block::exists(&mut conn, &old_block_id0).await.unwrap());
    assert!(block::exists(&mut conn, &new_block_id0).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_to_shorter() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator0 = random_head_locator(&mut rng);
    let locator1 = locator0.next();
    let locator2 = locator1.next();

    let content: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .take(3 * BLOCK_SIZE)
        .collect();

    let mut blob = Blob::create(branch.clone(), locator0, Shared::uninit());
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let new_len = BLOCK_SIZE / 2;

    blob.truncate(&mut conn, new_len as u64).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut buffer = vec![0; new_len];
    blob.seek(&mut conn, SeekFrom::Start(0)).await.unwrap();
    assert_eq!(blob.read(&mut conn, &mut buffer).await.unwrap(), new_len);
    assert_eq!(buffer, content[..new_len]);
    assert_eq!(blob.len().await, new_len as u64);

    trash_queue::flush_some(&mut conn, branch.keys(), u32::MAX)
        .await
        .unwrap();

    for locator in &[locator1, locator2] {
        assert_matches!(
            branch
                .data()
                .get(&mut conn, &locator.encode(branch.keys().read()))
                .await,
            Err(Error::EntryNotFound)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_marks_as_dirty() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator = random_head_locator(&mut rng);

    let content: Vec<_> = (&mut rng)
        .sample_iter(Standard)
        .take(2 * BLOCK_SIZE)
        .collect();

    let mut blob = Blob::create(branch.clone(), locator, Shared::uninit());
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    blob.truncate(&mut conn, 0).await.unwrap();
    assert!(blob.is_dirty());
}

#[tokio::test(flavor = "multi_thread")]
async fn modify_blob() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator0 = random_head_locator(&mut rng);
    let locator1 = locator0.next();

    let content = vec![0; 2 * BLOCK_SIZE];
    let mut blob = Blob::create(branch.clone(), locator0, Shared::uninit());
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let locator0 = locator0.encode(branch.keys().read());
    let locator1 = locator1.encode(branch.keys().read());

    let (old_block_id0, old_block_id1) = {
        let id0 = branch.data().get(&mut conn, &locator0).await.unwrap();
        let id1 = branch.data().get(&mut conn, &locator1).await.unwrap();
        (id0, id1)
    };

    let buffer = vec![1; 3 * BLOCK_SIZE / 2];
    blob.seek(&mut conn, SeekFrom::Start(0)).await.unwrap();
    blob.write(&mut conn, &buffer).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let new_block_id0 = branch.data().get(&mut conn, &locator0).await.unwrap();
    let new_block_id1 = branch.data().get(&mut conn, &locator1).await.unwrap();

    assert_ne!(new_block_id0, old_block_id0);
    assert_ne!(new_block_id1, old_block_id1);

    // Check the old blocks were deleted
    for block_id in &[old_block_id0, old_block_id1] {
        assert!(!block::exists(&mut conn, block_id).await.unwrap())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn append() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator = random_head_locator(&mut rng);
    let mut blob = Blob::create(branch.clone(), locator, Shared::uninit());
    blob.write(&mut conn, b"foo").await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut blob = Blob::open(&mut conn, branch.clone(), locator, Shared::uninit())
        .await
        .unwrap();

    blob.seek(&mut conn, SeekFrom::End(0)).await.unwrap();
    blob.write(&mut conn, b"bar").await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut blob = Blob::open(&mut conn, branch, locator, Shared::uninit())
        .await
        .unwrap();

    let content = blob.read_to_end(&mut conn).await.unwrap();
    assert_eq!(content, b"foobar");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_reopen_and_read() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let locator = random_head_locator(&mut rng);
    let shared = Shared::uninit();

    let mut blob = Blob::create(branch.clone(), locator, shared.clone());
    blob.write(&mut conn, b"foo").await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut blob = Blob::open(&mut conn, branch, locator, shared)
        .await
        .unwrap();

    let content = blob.read_to_end(&mut conn).await.unwrap();
    assert_eq!(content, b"foo");
}

#[proptest]
fn fork(
    #[strategy(0..2 * BLOCK_SIZE)] src_len: usize,
    #[strategy(0..=#src_len)] seek_pos: usize,
    #[strategy(1..BLOCK_SIZE)] write_len: usize,
    src_locator_is_root: bool,
    #[strategy(test_utils::rng_seed_strategy())] rng_seed: u64,
) {
    test_utils::run(fork_case(
        src_len,
        seek_pos,
        write_len,
        src_locator_is_root,
        rng_seed,
    ))
}

async fn fork_case(
    src_len: usize,
    seek_pos: usize,
    write_len: usize,
    src_locator_is_root: bool,
    rng_seed: u64,
) {
    let (mut rng, pool, src_branch) = setup(rng_seed).await;
    let mut conn = pool.acquire().await.unwrap();

    let (event_tx, _) = broadcast::channel(1);
    let dst_branch = Arc::new(
        BranchData::create(
            &mut conn,
            PublicKey::random(),
            src_branch.keys().write().unwrap(),
            event_tx.clone(),
        )
        .await
        .unwrap(),
    );
    let dst_branch = Branch::new(
        dst_branch,
        src_branch.keys().clone(),
        Arc::new(BlobCache::new(event_tx)),
    );

    let src_locator = if src_locator_is_root {
        Locator::ROOT
    } else {
        Locator::head(rng.gen())
    };

    let src_content: Vec<u8> = (&mut rng).sample_iter(Standard).take(src_len).collect();

    let mut blob = Blob::create(src_branch.clone(), src_locator, Shared::uninit());
    blob.write(&mut conn, &src_content[..]).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    blob.seek(&mut conn, SeekFrom::Start(seek_pos as u64))
        .await
        .unwrap();
    blob = blob.try_fork(&mut conn, dst_branch.clone()).await.unwrap();

    let write_content: Vec<u8> = rng.sample_iter(Standard).take(write_len).collect();

    blob.write(&mut conn, &write_content[..]).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    // Re-open the orig and verify the content is unchanged
    let mut orig = Blob::open(&mut conn, src_branch, src_locator, Shared::uninit())
        .await
        .unwrap();

    let buffer = orig.read_to_end(&mut conn).await.unwrap();
    assert_eq!(buffer.len(), src_content.len());
    assert!(buffer == src_content);

    // Re-open the fork and verify the content is changed
    let mut fork = Blob::open(&mut conn, dst_branch, src_locator, Shared::uninit())
        .await
        .unwrap();

    let mut buffer = vec![0; seek_pos];
    let len = fork.read(&mut conn, &mut buffer[..]).await.unwrap();
    assert_eq!(len, buffer.len());
    assert!(buffer == src_content[..seek_pos]);

    let mut buffer = vec![0; write_len];
    let len = fork.read(&mut conn, &mut buffer[..]).await.unwrap();
    assert_eq!(len, buffer.len());
    assert!(buffer == write_content);
}

// TODO: test that fork() doesn't create new blocks

#[tokio::test(flavor = "multi_thread")]
async fn block_ids_test() {
    let (mut rng, pool, branch) = setup(0).await;
    let mut conn = pool.acquire().await.unwrap();

    let blob_id: BlobId = rng.gen();
    let head_locator = Locator::head(blob_id);
    let mut blob = Blob::create(branch.clone(), head_locator, Shared::uninit());

    let content: Vec<_> = rng
        .sample_iter(Standard)
        .take(BLOCK_SIZE * 3 - HEADER_SIZE)
        .collect();
    blob.write(&mut conn, &content).await.unwrap();
    blob.flush(&mut conn).await.unwrap();

    let mut block_ids = BlockIds::new(branch, blob_id);
    let mut actual_count = 0;

    while block_ids.next(&mut conn).await.unwrap().is_some() {
        actual_count += 1;
    }

    assert_eq!(actual_count, 3);
}

async fn setup(rng_seed: u64) -> (StdRng, db::Pool, Branch) {
    let mut rng = StdRng::seed_from_u64(rng_seed);
    let secrets = WriteSecrets::generate(&mut rng);
    let pool = db::create(&db::Store::Temporary).await.unwrap();

    let (event_tx, _) = broadcast::channel(1);
    let branch = BranchData::create(
        &mut pool.acquire().await.unwrap(),
        PublicKey::random(),
        &secrets.write_keys,
        event_tx.clone(),
    )
    .await
    .unwrap();
    let branch = Branch::new(
        Arc::new(branch),
        secrets.into(),
        Arc::new(BlobCache::new(event_tx)),
    );

    (rng, pool, branch)
}

fn random_head_locator<R: Rng>(rng: &mut R) -> Locator {
    Locator::head(rng.gen())
}
