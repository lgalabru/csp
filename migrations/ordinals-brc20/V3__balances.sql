CREATE TABLE balances (
  ticker TEXT NOT NULL,
  address TEXT NOT NULL,
  avail_balance NUMERIC NOT NULL,
  trans_balance NUMERIC NOT NULL,
  total_balance NUMERIC NOT NULL
);
ALTER TABLE balances ADD PRIMARY KEY (ticker, address);
ALTER TABLE balances ADD CONSTRAINT balances_ticker_fk FOREIGN KEY(ticker) REFERENCES tokens(ticker) ON DELETE CASCADE;
CREATE INDEX balances_address_index ON balances (address);
CREATE INDEX balances_ticker_total_balance_index ON balances (ticker, total_balance DESC);
