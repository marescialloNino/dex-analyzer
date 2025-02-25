# src/utils/moralis_price_fetcher.py
import pandas as pd
import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

from src.models.price_bar import PriceBar

class MoralisPriceFetcher:
    def __init__(self):
        """Initialize Moralis price fetcher with Solana API endpoint."""
        load_dotenv()
        self.api_key = os.getenv("MORALIS_API_KEY")
        if not self.api_key:
            raise ValueError("Moralis API key not found in .env")
        self.base_url = "https://solana-gateway.moralis.io"  # Correct base URL
        self.headers = {
            "accept": "application/json",
            "X-API-Key": self.api_key
        }
        print(f"Initialized MoralisPriceFetcher with API key: {self.api_key[:4]}...")

    def get_price_bars(self, pair_address: str, token_symbol: str, from_date: str, to_date: str, timeframe: str = "1h", base_currency: str = "usd") -> PriceBar:
        """
        Fetch all OHLCV price bars for a Solana token pair with pagination.
        Args:
            pair_address: The liquidity pool address (e.g., from Meteora).
            token_symbol: Ticker symbol (e.g., 'BRAT-SOL').
            from_date, to_date: Dates in 'YYYY-MM-DD' or UNIX timestamp (seconds).
            timeframe: '1s', '10s', '30s', '1m', '5m', '10m', '30m', '1h', '4h', '12h', '1d', '1w', '1M'.
            base_currency: 'usd' or 'native'.
        """
        try:
            all_data = []
            cursor = None
            page = 1

            while True:
                # Build URL with cursor if available
                url = f"{self.base_url}/token/mainnet/pairs/{pair_address}/ohlcv?timeframe={timeframe}&baseCurrency={base_currency}&fromDate={from_date}&toDate={to_date}&limit=50"
                if cursor:
                    url += f"&cursor={cursor}"
                print(f"Requesting page {page}: {url}")
                
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code != 200:
                    print(f"Response text: {response.text}")
                response.raise_for_status()
                data = response.json()

                # Extract OHLCV data from 'result'
                if "result" not in data or not data["result"]:
                    print(f"No OHLCV data returned for pair {pair_address} on page {page}")
                    break
                all_data.extend(data["result"])

                # Check for next page
                cursor = data.get("cursor")
                page += 1
                if not cursor:
                    break  # No more pages

            if not all_data:
                raise ValueError(f"No data fetched for pair {pair_address}")

            # Process all collected OHLCV data
            df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume", "trades"])
            df = df[["timestamp", "open", "high", "low", "close", "volume"]]  # Drop 'trades' to match PriceBar
            df["timestamp"] = pd.to_datetime(df["timestamp"])  # ISO format from Moralis
            print(f"Columns after fetch: {df.columns.tolist()}")
            print(f"Fetched {len(df)} price bars for pair {pair_address} (ticker: {token_symbol}, timeframe: {timeframe})")
            return PriceBar(pair_address, base_currency, token_symbol, df)

        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            print(f"Price fetch failed: {e}, falling back to mock data")
            periods = {"1h": 168, "4h": 42, "1d": 7}.get(timeframe, 168)
            start_time = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime.now() - timedelta(days=7)
            df = pd.DataFrame({
                "timestamp": pd.date_range(start=start_time, periods=periods, freq=timeframe),
                "open": [100] * periods,
                "high": [100] * periods,
                "low": [100] * periods,
                "close": [100] * periods,
                "volume": [0] * periods
            })
            print(f"Mock columns: {df.columns.tolist()}")
            return PriceBar(pair_address, base_currency, token_symbol, df)