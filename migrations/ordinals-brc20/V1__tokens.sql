CREATE TABLE tokens (
    ticker TEXT NOT NULL PRIMARY KEY,
    inscription_id TEXT NOT NULL,
    inscription_number NUMERIC NOT NULL,
    block_height NUMERIC NOT NULL,
    block_hash TEXT NOT NULL,
    tx_id TEXT NOT NULL,
    tx_index BIGINT NOT NULL,
    address TEXT NOT NULL,
    max NUMERIC NOT NULL,
    "limit" NUMERIC,
    decimals INT NOT NULL,
    self_mint BOOLEAN NOT NULL DEFAULT FALSE,
    minted_supply NUMERIC DEFAULT 0,
    burned_supply NUMERIC DEFAULT 0,
    tx_count BIGINT DEFAULT 0,
    timestamp TIMESTAMPTZ NOT NULL
);
CREATE INDEX tokens_inscription_id_index ON tokens (inscription_id);
CREATE INDEX tokens_block_height_tx_index_index ON tokens (block_height DESC, tx_index DESC);
