CREATE TABLE inscription_recursions (
    inscription_id TEXT NOT NULL,
    ref_inscription_id TEXT NOT NULL
);
ALTER TABLE inscription_recursions ADD PRIMARY KEY (inscription_id, ref_inscription_id);
ALTER TABLE inscription_recursions ADD CONSTRAINT inscription_recursions_inscription_id_fk FOREIGN KEY(inscription_id) REFERENCES inscriptions(inscription_id) ON DELETE CASCADE;
