CREATE TABLE tokens (
    ticker TEXT NOT NULL PRIMARY KEY,
    genesis_id TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    tx_id TEXT NOT NULL,
    address TEXT NOT NULL,
    max NUMERIC NOT NULL,
    limit NUMERIC,
    decimals INT NOT NULL,
    self_mint BOOLEAN NOT NULL DEFAULT FALSE,
    minted_supply NUMERIC DEFAULT 0,
    burned_supply NUMERIC DEFAULT 0,
    tx_count BIGINT DEFAULT 0
);
CREATE INDEX tokens_genesis_id_index ON tokens (genesis_id);
CREATE INDEX tokens_block_height_index ON tokens (block_height);
