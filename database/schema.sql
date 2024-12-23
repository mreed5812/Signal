CREATE TABLE crypto_data (
    id INT PRIMARY KEY,
    name TEXT,
    symbol TEXT,
    cmc_rank INT,
    price NUMERIC,
    volume_24h NUMERIC,
    market_cap NUMERIC,
    market_cap_dominance NUMERIC,
    circulating_supply NUMERIC,
    max_supply NUMERIC,
    percent_change_1h NUMERIC,
    percent_change_24h NUMERIC,
    percent_change_7d NUMERIC,
    last_updated TIMESTAMP,
    data_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, last_updated)
);

CREATE TABLE gold_price_history (
    id SERIAL PRIMARY KEY,                  -- Auto-incrementing unique identifier
    timestamp TIMESTAMP NOT NULL,           -- Timestamp from the API response
    prev_close_price NUMERIC(10, 4),        -- Previous close price with 4 decimal precision
    open_price NUMERIC(10, 4),              -- Opening price with 4 decimal precision
    low_price NUMERIC(10, 4),               -- Lowest price of the period
    high_price NUMERIC(10, 4),              -- Highest price of the period
    price NUMERIC(10, 4) NOT NULL,          -- Current price of gold
    ch NUMERIC(10, 4),                      -- Absolute change in price
    chp NUMERIC(6, 2),                      -- Percentage change in price
    ask NUMERIC(10, 4),                     -- Ask price
    bid NUMERIC(10, 4),                     -- Bid price
    pulled_at TIMESTAMP NOT NULL            -- Timestamp when the data was pulled
);
