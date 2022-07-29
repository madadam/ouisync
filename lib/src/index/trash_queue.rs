use super::BranchData;
use crate::{
    access_control::AccessKeys,
    blob_id::BlobId,
    crypto::sign::PublicKey,
    db,
    error::{Error, Result},
    locator::Locator,
};
use sqlx::{Connection, Row};
use std::ops::Range;
use tokio::sync::broadcast;

// Max number of blocks to remove per one db acquire.
const BATCH_SIZE: u32 = 32;

/// Schedule blocks to be deleted.
pub(crate) async fn push(
    conn: &mut db::Connection,
    branch_id: &PublicKey,
    blob_id: &BlobId,
    range: Range<u32>,
) -> Result<()> {
    sqlx::query("INSERT INTO trash_queue (branch_id, blob_id, offset, count) VALUES (?, ?, ?, ?)")
        .bind(branch_id)
        .bind(blob_id)
        .bind(range.start)
        .bind(range.len() as u32)
        .execute(conn)
        .await?;

    Ok(())
}

/// Delete all blocks scheduled in the trash queue. Takes `Pool` instead of `Connection` to allow
/// periodically releasing the db connection to prevent blocking it for too long.
pub(crate) async fn flush_all(db: &db::Pool, access_keys: &AccessKeys) -> Result<()> {
    loop {
        let mut conn = db.acquire().await?;
        if !flush_some(&mut conn, access_keys, BATCH_SIZE).await? {
            break;
        }
    }

    Ok(())
}

/// Delete at most `limit` blocks from the trash queue. Returns whether there are still some blocks
/// remaining in the queue.
pub(crate) async fn flush_some(
    conn: &mut db::Connection,
    access_keys: &AccessKeys,
    mut limit: u32,
) -> Result<bool> {
    let write_keys = access_keys.write().ok_or(Error::PermissionDenied)?;

    let mut tx = conn.begin().await?;
    let mut removed = 0;

    while limit > 0 {
        let row =
            sqlx::query("SELECT rowid, branch_id, blob_id, offset, count FROM trash_queue LIMIT 1")
                .fetch_optional(&mut *tx)
                .await?;

        let row = if let Some(row) = row {
            row
        } else {
            break;
        };

        let rowid: u32 = row.get(0);
        let branch_id = row.get(1);
        let blob_id = row.get(2);
        let offset: u32 = row.get(3);
        let count: u32 = row.get(4);

        let (event_tx, _) = broadcast::channel(1);
        let branch = BranchData::new(branch_id, event_tx);
        let locator = Locator::head(blob_id);

        let to_remove = count.min(limit);
        let first = offset + count - to_remove;
        let last = offset + count;

        for number in first..last {
            let locator = locator.nth(number);
            let locator = locator.encode(access_keys.read());

            match branch.remove(&mut tx, &locator, write_keys).await {
                Ok(()) | Err(Error::EntryNotFound) => (),
                Err(error) => return Err(error),
            }
        }

        removed += to_remove;
        limit -= to_remove;

        let new_count = count - to_remove;

        if new_count > 0 {
            sqlx::query("UPDATE trash_queue SET count = {} WHERE rowid = ?")
                .bind(new_count)
                .bind(rowid)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("DELETE FROM trash_queue WHERE rowid = ?")
                .bind(rowid)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit().await?;

    Ok(removed >= BATCH_SIZE)
}
