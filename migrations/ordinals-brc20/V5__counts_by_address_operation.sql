CREATE TABLE counts_by_address_operation (
    address TEXT NOT NULL,
    operation TEXT NOT NULL,
    count INT NOT NULL DEFAULT 0
);
ALTER TABLE counts_by_address_operation ADD PRIMARY KEY (address, operation);
