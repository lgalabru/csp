CREATE TABLE current_locations (
    ordinal_number NUMERIC NOT NULL UNIQUE,
    block_height BIGINT NOT NULL,
    tx_index BIGINT NOT NULL,
    address TEXT NOT NULL
);
CREATE INDEX current_locations_address_index ON current_locations (address);
CREATE INDEX current_locations_block_height_tx_index_index ON current_locations (block_height, tx_index);
