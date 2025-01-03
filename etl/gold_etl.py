import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

class GoldETLProcessor:
    def __init__(self, database_url, table_name):
        self.database_url = database_url
        self.table_name = table_name
        self.engine = create_engine(database_url)

    def fetch_data(self, url, headers):
        """
        Fetch data from the Gold API.
        """
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None

    def process_data(self, raw_data):
        """
        Process raw gold price data into a DataFrame.
        """
        processed_data = {
            "timestamp": datetime.utcfromtimestamp(raw_data["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
            "prev_close_price": raw_data["prev_close_price"],
            "open_price": raw_data["open_price"],
            "low_price": raw_data["low_price"],
            "high_price": raw_data["high_price"],
            "price": raw_data["price"],
            "ch": raw_data["ch"],
            "chp": raw_data["chp"],
            "ask": raw_data["ask"],
            "bid": raw_data["bid"],
            "pulled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        df = pd.DataFrame([processed_data])
        return self.validate_and_truncate(df)

    def validate_and_truncate(self, df):
        """
        Validate and truncate data to match database constraints.
        """
        df = df.dropna()
        return df

    def upsert_data(self, df):
        """
        Upsert data into the database table.
        """
        if df.empty:
            print("No data to insert.")
            return

        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        table = Table(self.table_name, metadata, autoload_with=self.engine)

        records = df.to_dict(orient="records")

        stmt = insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing()

        with self.engine.connect() as connection:
            try:
                result = connection.execute(stmt)
                connection.commit()  # Ensure changes are committed to the database
                print(f"Upsert completed. {result.rowcount} new records inserted.")
            except Exception as e:
                print(f"Error during upsert: {e}")

    def run(self, url, headers):
        """
        Run the ETL pipeline: fetch, process, validate, and load data.
        """
        raw_data = self.fetch_data(url, headers)
        if raw_data:
            df = self.process_data(raw_data)
            self.upsert_data(df)

# Usage example
def main():
    api_key = "goldapi-eooasm506kn11-io"
    url = "https://www.goldapi.io/api/XAU/USD"
    headers = {
        "x-access-token": api_key,
        "Content-Type": "application/json"
    }

    database_url = "postgresql+psycopg2://postgres@localhost:5432/signal"
    table_name = "gold_price_history"

    etl = GoldETLProcessor(database_url, table_name)
    etl.run(url, headers)

if __name__ == "__main__":
    main()
