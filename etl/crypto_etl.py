#!/usr/bin/env python
# coding: utf-8

import requests # type: ignore
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from datetime import datetime

# ETL Pipeline for loading cryptocurrency data

# 1. API Call to Fetch Cryptocurrency Data
def fetch_crypto_data(api_key, url, parameters, headers):
    try:
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# 2. Process the Raw Data into a DataFrame
def process_crypto_data(raw_data):
    organized_data = []
    for item in raw_data.get('data', []):
        btc_info = {
            "id": item['id'],
            "name": item['name'],
            "symbol": item['symbol'],
            "cmc_rank": item['cmc_rank'],
            "price": item['quote']['USD']['price'],
            "volume_24h": item['quote']['USD']['volume_24h'],
            "market_cap": item['quote']['USD']['market_cap'],
            "market_cap_dominance": item['quote']['USD']['market_cap_dominance'],
            "circulating_supply": item['circulating_supply'],
            "max_supply": item['max_supply'],
            "percent_change_1h": item['quote']['USD']['percent_change_1h'],
            "percent_change_24h": item['quote']['USD']['percent_change_24h'],
            "percent_change_7d": item['quote']['USD']['percent_change_7d'],
            "last_updated": item['quote']['USD']['last_updated']
        }
        organized_data.append(btc_info)
    df = pd.DataFrame(organized_data)
    # Convert last_updated to datetime and add pulled_at
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    df["pulled_at"] = datetime.now()
    return df

# 3. Validate and Truncate Data to Match Database Constraints
def validate_and_truncate(df):
    df["symbol"] = df["symbol"].str[:10]
    df["name"] = df["name"].str[:50]
    df["price"] = df["price"].clip(upper=10**12 - 1)
    df["volume_24h"] = df["volume_24h"].clip(upper=10**12 - 1)
    df["market_cap"] = df["market_cap"].clip(upper=10**12 - 1)
    df["market_cap_dominance"] = df["market_cap_dominance"].clip(upper=999.99)
    df["circulating_supply"] = df["circulating_supply"].clip(upper=10**12 - 1)
    df["max_supply"] = df["max_supply"].clip(upper=10**12 - 1)
    df["percent_change_1h"] = df["percent_change_1h"].clip(-9999.9999, 9999.9999)
    df["percent_change_24h"] = df["percent_change_24h"].clip(-9999.9999, 9999.9999)
    df["percent_change_7d"] = df["percent_change_7d"].clip(-9999.9999, 9999.9999)
    df = df.dropna(subset=["symbol", "last_updated"])
    return df

# 4. Upsert Data into the Database
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert

def upsert_crypto_data(df, table_name, engine):
    """
    Upsert data into the database table, ensuring no duplicate records are inserted.
    Records are considered unique based on 'symbol' and 'last_updated'.
    """
    # Drop the 'id' column if it exists, as it should be auto-generated by the database
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # Convert 'last_updated' to UTC timezone to ensure consistency
    df["last_updated"] = pd.to_datetime(df["last_updated"]).dt.tz_convert("UTC")

    # Convert DataFrame to a list of dictionaries for bulk insert
    records = df.to_dict(orient="records")

    # Create a MetaData instance and reflect the table schema
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = Table(table_name, metadata, autoload_with=engine)  # Define the table object

    # Prepare the insert statement with ON CONFLICT logic
    stmt = insert(table).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "last_updated"])

    # Execute the upsert operation
    with engine.connect() as connection:
        try:
            result = connection.execute(stmt)
            print(f"Upsert completed. {result.rowcount} new records inserted.")
        except Exception as e:
            print(f"Error during upsert: {e}")


# Main function to execute ETL pipeline
def main():
    # API Setup
    api_key = '42485936-1986-4342-9e0a-e854c8b0fe47'
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {'start': '1', 'limit': '100', 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': api_key}

    # Database Configuration
    DB_USER = "postgres"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "signal"
    table_name = "crypto_price_history"
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DATABASE_URL)

    # Fetch, Process, Validate, and Load Data
    raw_data = fetch_crypto_data(api_key, url, parameters, headers)
    if raw_data:
        df = process_crypto_data(raw_data)
        df_cleaned = validate_and_truncate(df)
        upsert_crypto_data(df_cleaned, table_name, engine)

if __name__ == "__main__":
    main()

