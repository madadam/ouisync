-- Blocks to be deleted
CREATE TABLE IF NOT EXISTS trash_queue (
    branch_id BLOB    NOT NULL,
    -- Id of the blob to be deleted/truncated
    blob_id   BLOB    NOT NULL,
    -- Index of the first block to be deleted
    offset    INTEGER NOT NULL,
    -- Number of blocks to delete
    count     INTEGER NOT NULL
);
