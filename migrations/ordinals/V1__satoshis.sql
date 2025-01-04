CREATE TABLE satoshis (
    ordinal_number NUMERIC NOT NULL PRIMARY KEY,
    rarity TEXT NOT NULL,
    coinbase_height NUMERIC NOT NULL
);
CREATE INDEX satoshis_rarity_index ON satoshis (rarity);
