CREATE TABLE counts_by_mime_type (
    mime_type TEXT NOT NULL PRIMARY KEY,
    count INT NOT NULL DEFAULT 0
);
