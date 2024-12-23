from etl_processor import ETLProcessor
import pandas as pd
from datetime import datetime

class CryptoETLProcessor(ETLProcessor):
    def process_data(self, raw_data):
        """
        Process raw cryptocurrency data into a DataFrame.
        """
        organized_data = []
        for item in raw_data.get('data', []):
            btc_info = {
                "id": item['id'],
                "symbol": item['symbol'],
                "name": item['name'],
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
                "last_updated": pd.to_datetime(item['quote']['USD']['last_updated']),
                "pulled_at": datetime.now()
            }
            organized_data.append(btc_info)

        df = pd.DataFrame(organized_data)
        return self.validate_and_truncate(df)

    def validate_and_truncate(self, df):
        """
        Validate and truncate cryptocurrency data fields to match database constraints.
        """
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

# Usage example
def main():
    api_key = '42485936-1986-4342-9e0a-e854c8b0fe47'
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {'start': '1', 'limit': '100', 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': api_key}

    database_url = "postgresql+psycopg2://postgres@localhost:5432/signal"
    table_name = "crypto_price_history"

    etl = CryptoETLProcessor(database_url, table_name)
    etl.run(url, parameters, headers)

if __name__ == "__main__":
    main()
