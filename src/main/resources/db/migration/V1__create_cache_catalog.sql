CREATE TABLE IF NOT EXISTS cache_catalog (
    cacheId            TEXT PRIMARY KEY,
    dasId              TEXT NOT NULL,
    tableId            TEXT NOT NULL,
    columns            TEXT NOT NULL,     -- comma-separated list of columns
    quals              BLOB NOT NULL,     -- repeated Qual stored as a delimited stream
    sortKeys           BLOB NOT NULL,     -- repeated SortKeys stored as a delimited stream
    state              TEXT NOT NULL,
    stateDetail        TEXT,
    creationDate       TEXT NOT NULL,
    lastAccessDate     TEXT,
    activeReaders      INTEGER NOT NULL DEFAULT 0,
    numberOfTotalReads INTEGER NOT NULL DEFAULT 0,
    sizeInBytes        INTEGER
);
