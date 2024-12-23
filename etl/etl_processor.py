import requests
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

class ETLProcessor:
    def __init__(self, database_url, table_name):
        self.database_url = database_url
        self.table_name = table_name
        self.engine = create_engine(database_url)

    def fetch_data(self, url, parameters, headers):
        """
        Fetch data from an API.
        """
        try:
            response = requests.get(url, headers=headers, params=parameters)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None

    def process_data(self, raw_data):
        """
        Process raw data into a DataFrame. Must be overridden by subclasses for specific transformations.
        """
        raise NotImplementedError("process_data() must be implemented by subclasses")

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

    def run(self, url, parameters, headers):
        """
        Run the ETL pipeline: fetch, process, validate, and load data.
        """
        raw_data = self.fetch_data(url, parameters, headers)
        if raw_data:
            df = self.process_data(raw_data)
            df_cleaned = self.validate_and_truncate(df)
            self.upsert_data(df_cleaned)