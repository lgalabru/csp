CREATE TABLE inscription_transfers (
    inscription_id TEXT NOT NULL,
    number BIGINT NOT NULL,
    ordinal_number NUMERIC NOT NULL,
    block_height NUMERIC NOT NULL,
    tx_index BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    block_transfer_index INT NOT NULL
);
ALTER TABLE inscription_transfers ADD PRIMARY KEY (block_height, block_transfer_index);
CREATE INDEX inscription_transfers_inscription_id_index ON inscription_transfers (inscription_id);
CREATE INDEX inscription_transfers_number_index ON inscription_transfers (number);
