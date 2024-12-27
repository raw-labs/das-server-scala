CREATE TABLE IF NOT EXISTS cache_catalog (
    cacheId            TEXT PRIMARY KEY,
    dasId              TEXT NOT NULL,
    definition         TEXT NOT NULL,
    state              TEXT NOT NULL,
    stateDetail        TEXT,
    creationDate       TEXT NOT NULL,
    lastAccessDate     TEXT,
    numberOfTotalReads INTEGER NOT NULL DEFAULT 0,
    sizeInBytes        INTEGER
);
