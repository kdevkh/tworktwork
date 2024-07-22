import pandas as pd
from binance.client import Client
from datetime import datetime
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataLoader, cls).__new__(cls)
            cls._instance.client = Client(tld="com")
            cls._instance.api_calls = 0
        return cls._instance

    def get_history(self, symbol, interval, start, end=None):
        self.api_calls = 0
        start_time = time.time()
        
        logger.info(f"Starting data retrieval for {symbol} from {start} to {end}")
        
        bars = []
        start_ts = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end, "%Y-%m-%d").timestamp() * 1000) if end else None

        while True:
            self.api_calls += 1
            logger.info(f"Making API call #{self.api_calls}")
            
            new_bars = self.client.get_historical_klines(
                symbol=symbol, 
                interval=interval,
                start_str=start_ts,
                end_str=end_ts,
                limit=1000
            )
            
            bars.extend(new_bars)
            
            if len(new_bars) < 1000:
                break
            
            start_ts = new_bars[-1][0] + 1

        df = pd.DataFrame(bars)
        df["Date"] = pd.to_datetime(df.iloc[:, 0], unit="ms")
        df.columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                      "Close Time", "Quote Asset Volume", "Number of Trades",
                      "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore", "Date"]
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        df.set_index("Date", inplace=True)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        end_time = time.time()
        logger.info(f"Data retrieval completed. Total API calls: {self.api_calls}")
        logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
        logger.info(f"Total data points retrieved: {len(df)}")

        return df

    def get_earliest_valid_timestamp(self, symbol, interval):
        timestamp = self.client._get_earliest_valid_timestamp(symbol=symbol, interval=interval)
        return pd.to_datetime(timestamp, unit="ms")

    # def save_data_to_csv(self, df, symbol):
    #     now = datetime.now()
    #     today = now.strftime("%Y%m%d")
    #     current_time = now.strftime("%H%M%S")
    #     folder_path = os.path.join("data", "raw", today)
    #     os.makedirs(folder_path, exist_ok=True)

    #     file_name = f"{symbol}_{today}_{current_time}.csv"
    #     file_path = os.path.join(folder_path, file_name)
    #     df.to_csv(file_path)
    #     print(f"Data saved to {file_path}")

def get_data(client, symbol, interval, start, end=None):
    loader = DataLoader()
    df = loader.get_history(symbol, interval, start, end)
    logger.info(f"Total API calls made: {loader.api_calls}")
    return df

def get_earliest_timestamp(client, symbol, interval):
    loader = DataLoader()
    return loader.get_earliest_valid_timestamp(symbol, interval)