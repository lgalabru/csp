CREATE TYPE transfer_type AS ENUM ('transferred', 'spent_in_fees', 'burnt');
CREATE TABLE locations (
    ordinal_number NUMERIC NOT NULL,
    block_height NUMERIC NOT NULL,
    tx_index BIGINT NOT NULL,
    tx_id TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    address TEXT NOT NULL,
    output TEXT NOT NULL,
    "offset" NUMERIC,
    prev_output TEXT,
    prev_offset NUMERIC,
    value NUMERIC,
    transfer_type transfer_type NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);
ALTER TABLE locations ADD PRIMARY KEY (ordinal_number, block_height, tx_index);
CREATE INDEX locations_output_offset_index ON locations (output, "offset");
CREATE INDEX locations_timestamp_index ON locations (timestamp);
CREATE INDEX locations_block_height_tx_index_index ON locations (block_height DESC, tx_index DESC);
