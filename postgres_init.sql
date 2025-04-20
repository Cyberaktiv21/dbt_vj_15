CREATE TABLE IF NOT EXISTS coin_prices (
    ts      TIMESTAMP PRIMARY KEY,
    symbol  TEXT NOT NULL,
    price   NUMERIC
);

CREATE TABLE IF NOT EXISTS coin_update_counts (
    window_start TIMESTAMP,
    symbol       TEXT,
    updates      INTEGER,
    PRIMARY KEY (window_start, symbol)
);