CREATE TABLE inscription_recursions (
    genesis_id TEXT NOT NULL,
    ref_genesis_id TEXT NOT NULL
);
ALTER TABLE inscription_recursions ADD PRIMARY KEY (genesis_id, ref_genesis_id);
ALTER TABLE inscription_recursions ADD CONSTRAINT inscription_recursions_genesis_id_fk FOREIGN KEY(genesis_id) REFERENCES inscriptions(genesis_id) ON DELETE CASCADE;
