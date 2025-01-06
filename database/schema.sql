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

CREATE TABLE treasury_yields (
    date DATE PRIMARY KEY,       -- The date of the yield
    value DECIMAL(10, 4) NOT NULL, -- Yield value, with up to 4 decimal precision
    pulled_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Tracks when the data was pulled
);

CREATE TABLE federal_funds_rate (
    date DATE PRIMARY KEY,       -- The date of the yield
    value DECIMAL(10, 4) NOT NULL, -- Yield value, with up to 4 decimal precision
    pulled_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Tracks when the data was pulled
);

CREATE TABLE sp500_index_data (
    date DATE PRIMARY KEY,        -- The date of the data
    open DECIMAL(10, 4) NOT NULL, -- Opening price
    high DECIMAL(10, 4) NOT NULL, -- Highest price of the day
    low DECIMAL(10, 4) NOT NULL,  -- Lowest price of the day
    close DECIMAL(10, 4) NOT NULL,-- Closing price
    volume BIGINT NOT NULL,       -- Volume of trades
    pulled_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the data was pulled
);

CREATE TABLE news_sentiment (
    title TEXT NOT NULL,                     -- Title of the news article
    url TEXT NOT NULL,                       -- URL of the news article
    time_published TIMESTAMP NOT NULL,       -- Time the news was published
    authors TEXT[],                          -- Array of authors
    summary TEXT,                            -- Summary of the article
    banner_image TEXT,                       -- URL of the banner image
    source TEXT NOT NULL,                    -- Source of the article (e.g., Benzinga)
    category_within_source TEXT,             -- Category within the source (e.g., News)
    source_domain TEXT,                      -- Domain of the source (e.g., www.benzinga.com)
    topics JSONB,                            -- Topics as JSON array (e.g., [{'topic': 'Technology', 'relevance_score': '1.0'}])
    overall_sentiment_score NUMERIC(10, 6),  -- Sentiment score (e.g., 0.182901)
    overall_sentiment_label TEXT,            -- Sentiment label (e.g., Bullish, Neutral)
    ticker_sentiment JSONB,                  -- Ticker sentiment as JSON array (e.g., [{'ticker': 'META', 'relevance_score': '0.0621'}])
    pulled_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the data was pulled
);
