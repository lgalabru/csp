CREATE TABLE counts_by_block (
    block_height NUMERIC NOT NULL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    inscription_count BIGINT NOT NULL,
    inscription_count_accum BIGINT NOT NULL,
    timestamp BIGINT NOT NULL
);
CREATE INDEX counts_by_block_block_hash_index ON counts_by_block (block_hash);
