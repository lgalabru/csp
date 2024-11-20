CREATE TABLE inscriptions (
    inscription_id TEXT NOT NULL PRIMARY KEY,
    ordinal_number NUMERIC NOT NULL,
    number BIGINT NOT NULL UNIQUE,
    classic_number BIGINT NOT NULL,
    block_height NUMERIC NOT NULL,
    block_hash TEXT NOT NULL,
    tx_index BIGINT NOT NULL,
    address TEXT,
    mime_type TEXT NOT NULL,
    content_type TEXT NOT NULL,
    content_length BIGINT NOT NULL,
    content BYTEA NOT NULL,
    fee NUMERIC NOT NULL,
    curse_type TEXT,
    recursive BOOLEAN DEFAULT FALSE,
    input_index BIGINT NOT NULL,
    pointer NUMERIC,
    metadata TEXT,
    metaprotocol TEXT,
    parent TEXT,
    delegate TEXT,
    timestamp BIGINT NOT NULL
);
CREATE INDEX inscriptions_mime_type_index ON inscriptions (mime_type);
CREATE INDEX inscriptions_recursive_index ON inscriptions (recursive);
CREATE INDEX inscriptions_block_height_tx_index_index ON inscriptions (block_height DESC, tx_index DESC);
CREATE INDEX inscriptions_address_index ON inscriptions (address);
CREATE INDEX inscriptions_ordinal_number_index ON inscriptions (ordinal_number);
