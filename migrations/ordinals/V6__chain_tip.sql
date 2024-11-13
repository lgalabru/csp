CREATE TABLE chain_tip (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    block_height NUMERIC NOT NULL 
);
ALTER TABLE chain_tip ADD CONSTRAINT chain_tip_one_row CHECK(id);
