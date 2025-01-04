CREATE TABLE current_locations (
    ordinal_number NUMERIC NOT NULL PRIMARY KEY,
    block_height NUMERIC NOT NULL,
    tx_id TEXT NOT NULL,
    tx_index BIGINT NOT NULL,
    address TEXT,
    output TEXT NOT NULL,
    "offset" NUMERIC
);
CREATE INDEX current_locations_address_index ON current_locations (address);
CREATE INDEX current_locations_block_height_tx_index_index ON current_locations (block_height, tx_index);
CREATE INDEX current_locations_output_index ON current_locations (output);
