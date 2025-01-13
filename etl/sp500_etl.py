import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert


class SP500ETLProcessor:
    def __init__(self, database_url, table_name):
        self.database_url = database_url
        self.table_name = table_name
        self.engine = create_engine(database_url)

    def fetch_data(self, url):
        """
        Fetch data from the Alpha Vantage API.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None

    def process_data(self, raw_data):
        """
        Process raw S&P 500 data into a DataFrame.
        """
        try:
            time_series = raw_data['Time Series (Daily)']
            df = pd.DataFrame.from_dict(time_series, orient='index')

            # Rename columns to match database schema
            df.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            }, inplace=True)

            # Convert index to datetime and reset index
            df.index = pd.to_datetime(df.index)
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            # Ensure numerical columns are of the correct type
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Add a pulled_on timestamp
            df['pulled_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return self.validate_and_truncate(df)
        except KeyError as e:
            print(f"Error processing data: Missing key {e}")
            return pd.DataFrame()

    def validate_and_truncate(self, df):
        """
        Validate and truncate data to match database constraints.
        """
        df = df.dropna()  # Drop rows with missing values
        df['date'] = df['date'].dt.strftime("%Y-%m-%d")  # Format date
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

    def run(self, url):
        """
        Run the ETL pipeline: fetch, process, validate, and load data.
        """
        raw_data = self.fetch_data(url)
        if raw_data:
            df = self.process_data(raw_data)
            self.upsert_data(df)


# Usage example
def main():
    api_key = "SFRHBUTCXB3RDG5S"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VOO&apikey={api_key}"

    database_url = "postgresql+psycopg2://postgres@localhost:5432/signal"
    table_name = "sp500_index_data"

    etl = SP500ETLProcessor(database_url, table_name)
    etl.run(url)


if __name__ == "__main__":
    main()
