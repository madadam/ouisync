use super::{BlockId, BLOCK_SIZE};
use crate::{
    db,
    error::{Error, Result},
};
use sqlx::{sqlite::SqliteRow, Row};

pub(crate) const BLOCK_NONCE_SIZE: usize = 32;
pub(crate) type BlockNonce = [u8; BLOCK_NONCE_SIZE];

/// Initializes the block store. Creates the required database schema unless already exists.
pub async fn init(conn: &mut db::Connection) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS blocks (
             id       BLOB NOT NULL PRIMARY KEY,
             nonce    BLOB NOT NULL,
             content  BLOB NOT NULL
         ) WITHOUT ROWID",
    )
    .execute(conn)
    .await
    .map_err(Error::CreateDbSchema)?;

    Ok(())
}

/// Reads a block from the store into a buffer.
///
/// # Panics
///
/// Panics if `buffer` length is less than [`BLOCK_SIZE`].
pub(crate) async fn read(
    conn: &mut db::Connection,
    id: &BlockId,
    buffer: &mut [u8],
) -> Result<BlockNonce> {
    let row = sqlx::query("SELECT nonce, content FROM blocks WHERE id = ?")
        .bind(id)
        .fetch_optional(conn)
        .await?
        .ok_or(Error::BlockNotFound(*id))?;

    from_row(row, buffer)
}

fn from_row(row: SqliteRow, buffer: &mut [u8]) -> Result<BlockNonce> {
    assert!(
        buffer.len() >= BLOCK_SIZE,
        "insufficient buffer length for block read"
    );

    let nonce: &[u8] = row.get(0);
    let nonce = BlockNonce::try_from(nonce)?;

    let content: &[u8] = row.get(1);
    if content.len() != BLOCK_SIZE {
        return Err(Error::WrongBlockLength(content.len()));
    }

    buffer.copy_from_slice(content);

    Ok(nonce)
}

/// Writes a block into the store.
///
/// If a block with the same id already exists, this is a no-op.
///
/// # Panics
///
/// Panics if buffer length is not equal to [`BLOCK_SIZE`].
///
pub(crate) async fn write(
    conn: &mut db::Connection,
    id: &BlockId,
    buffer: &[u8],
    nonce: &BlockNonce,
) -> Result<()> {
    assert_eq!(
        buffer.len(),
        BLOCK_SIZE,
        "incorrect buffer length for block write"
    );

    sqlx::query(
        "INSERT INTO blocks (id, nonce, content)
         VALUES (?, ?, ?)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .bind(nonce.as_slice())
    .bind(buffer)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Checks whether a block exists in the store.
/// (Currently used only in tests)
#[cfg(test)]
pub(crate) async fn exists(conn: &mut db::Connection, id: &BlockId) -> Result<bool> {
    Ok(sqlx::query("SELECT 0 FROM blocks WHERE id = ?")
        .bind(id)
        .fetch_optional(conn)
        .await?
        .is_some())
}

/// Returns the total number of blocks in the store.
pub(crate) async fn count(conn: &mut db::Connection) -> Result<usize> {
    Ok(db::decode_u64(
        sqlx::query("SELECT COUNT(*) FROM blocks")
            .fetch_one(conn)
            .await?
            .get(0),
    ) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[tokio::test(flavor = "multi_thread")]
    async fn write_and_read() {
        let mut conn = setup().await;

        let content = random_block_content();
        let id = BlockId::from_content(&content);
        let nonce = BlockNonce::default();

        write(&mut conn, &id, &content, &nonce).await.unwrap();

        let mut buffer = vec![0; BLOCK_SIZE];
        read(&mut conn, &id, &mut buffer).await.unwrap();

        assert_eq!(buffer, content);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_read_missing_block() {
        let mut conn = setup().await;

        let mut buffer = vec![0; BLOCK_SIZE];
        let id = BlockId::from_content(&buffer);

        match read(&mut conn, &id, &mut buffer).await {
            Err(Error::BlockNotFound(missing_id)) => assert_eq!(missing_id, id),
            Err(error) => panic!("unexpected error: {:?}", error),
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_write_existing_block() {
        let mut conn = setup().await;

        let content0 = random_block_content();
        let id = BlockId::from_content(&content0);
        let nonce = BlockNonce::default();

        write(&mut conn, &id, &content0, &nonce).await.unwrap();
        write(&mut conn, &id, &content0, &nonce).await.unwrap();
    }

    async fn setup() -> db::Connection {
        let mut conn = db::open_or_create(&db::Store::Temporary)
            .await
            .unwrap()
            .acquire()
            .await
            .unwrap()
            .detach();
        init(&mut conn).await.unwrap();
        conn
    }

    fn random_block_content() -> Vec<u8> {
        let mut content = vec![0; BLOCK_SIZE];
        rand::thread_rng().fill(&mut content[..]);
        content
    }
}
