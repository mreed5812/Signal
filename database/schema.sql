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
