import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert


class NewsSentimentETLProcessor:
    def __init__(self, database_url, table_name):
        self.database_url = database_url
        self.table_name = table_name
        self.engine = create_engine(database_url)

    def fetch_data(self, url):
        """
        Fetch data from the Alpha Vantage News Sentiment API.
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
        Process raw news sentiment data into a DataFrame.
        """
        try:
            # Extract the 'feed' data
            feed_data = raw_data['feed']

            # Convert to a DataFrame
            df = pd.DataFrame(feed_data)

            # Add a pulled_on timestamp
            df['pulled_on'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Parse and format the 'time_published' field to match database format
            df['time_published'] = pd.to_datetime(df['time_published'], format='%Y%m%dT%H%M%S')

            # Ensure the numerical column is properly formatted
            df['overall_sentiment_score'] = pd.to_numeric(df['overall_sentiment_score'], errors='coerce')

            # Drop unnecessary fields or parse them if needed
            # For example, flattening 'topics' or 'ticker_sentiment' could be done here if required

            # Validate and truncate
            return self.validate_and_truncate(df)
        except KeyError as e:
            print(f"Error processing data: Missing key {e}")
            return pd.DataFrame()

    def validate_and_truncate(self, df):
        """
        Validate and truncate data to match database constraints.
        """
        df = df.dropna(subset=['title', 'url', 'time_published', 'overall_sentiment_score'])
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
    url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&CRYPTO:BTC&apikey={api_key}"

    database_url = "postgresql+psycopg2://postgres@localhost:5432/signal"
    table_name = "news_sentiment"

    etl = NewsSentimentETLProcessor(database_url, table_name)
    etl.run(url)


if __name__ == "__main__":
    main()
