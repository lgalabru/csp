CREATE TYPE operation AS ENUM ('deploy', 'mint', 'transfer', 'transfer_send', 'transfer_receive');
CREATE TABLE operations (
    ticker TEXT NOT NULL,
    operation operation NOT NULL,
    inscription_id TEXT NOT NULL,
    inscription_number BIGINT NOT NULL,
    ordinal_number NUMERIC NOT NULL,
    block_height NUMERIC NOT NULL,
    block_hash TEXT NOT NULL,
    tx_id TEXT NOT NULL,
    tx_index NUMERIC NOT NULL,
    output TEXT NOT NULL,
    "offset" NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    address TEXT NOT NULL,
    to_address TEXT,
    amount NUMERIC NOT NULL
);
ALTER TABLE operations ADD PRIMARY KEY (inscription_id, operation);
ALTER TABLE operations ADD CONSTRAINT operations_ticker_fk FOREIGN KEY(ticker) REFERENCES tokens(ticker) ON DELETE CASCADE;
CREATE INDEX operations_operation_index ON operations (operation);
CREATE INDEX operations_ticker_address_index ON operations (ticker, address);
CREATE INDEX operations_block_height_tx_index_index ON operations (block_height DESC, tx_index DESC);
CREATE INDEX operations_address_to_address_index ON operations (address, to_address);
CREATE INDEX operations_ordinal_number_operation_index ON operations (ordinal_number, operation);
