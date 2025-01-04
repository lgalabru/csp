CREATE TABLE counts_by_genesis_address (
    address TEXT NOT NULL PRIMARY KEY,
    count INT NOT NULL DEFAULT 0
);
