import pandas as pd
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import os

from src.models.price_bar import PriceBar

class CoinGeckoPriceFetcher:
    def __init__(self):
        """Initialize CoinGecko price fetcher with explicit free-tier URL."""
        load_dotenv()
        self.api_key = os.getenv("COINGECKO_API_KEY")
        self.base_url = "https://api.coingecko.com/api/v3"
        self.headers = {"accept": "application/json"}
        if self.api_key:
            self.headers["x-cg-demo-api-key"] = self.api_key
            print(f"Using CoinGecko demo API key: {self.api_key[:4]}...")
        
        self.mint_to_cg_id = {
            "So11111111111111111111111111111111111111112": "solana",
            "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": "jupiter-exchange-solana",
            "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn": "jito-staked-sol",
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": "marinade-staked-sol",
            "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1": "blaze-staked-sol",
            "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij": "coinbase-wrapped-btc",
            "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh": "wrapped-btc-wormhole"
        }
        self._load_coin_list()

    def _load_coin_list(self):
        print("Fetching CoinGecko coin list for mint mapping...")
        try:
            url = f"{self.base_url}/coins/list?include_platform=true"
            print(f"Requesting: {url} with headers: {self.headers}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            coins = response.json()
            print(f"Received {len(coins)} coins from CoinGecko")
            if not coins:
                print("Warning: Coin list is empty, using fallback mappings")
                return

            print("Sample coin entries:", coins[:3])
            for coin in coins:
                platforms = coin.get("platforms", {})
                solana_mint = platforms.get("solana", "")
                if solana_mint:
                    self.mint_to_cg_id[solana_mint] = coin["id"]
            print(f"Mapped {len(self.mint_to_cg_id)} Solana mints to CoinGecko IDs")
            for key_mint in ["So11111111111111111111111111111111111111112", "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"]:
                if key_mint in self.mint_to_cg_id:
                    print(f"Mint {key_mint[:8]}... mapped to: {self.mint_to_cg_id[key_mint]}")
                else:
                    print(f"Mint {key_mint[:8]}... not dynamically mapped, using fallback")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch coin list: {e}, using fallback mappings")

    def get_price_bars(self, token_address: str, base_currency: str = "usd", days: int = 7, token_symbol: str = "UNKNOWN") -> PriceBar:
        """Fetch 7-day OHLC price bars from CoinGecko using token mint address (4-hour intervals).
        possible days: 1,2,7,14,30,..."""
        try:
            cg_id = self.mint_to_cg_id.get(token_address)
            if not cg_id:
                raise ValueError(f"No CoinGecko ID found for mint {token_address}")

            print(f"Fetching price history for {cg_id} (mint: {token_address}, ticker: {token_symbol}) from CoinGecko")
            url = f"{self.base_url}/coins/{cg_id}/ohlc?vs_currency={base_currency.lower()}&days={days}"
            print(f"Requesting: {url} with headers: {self.headers}")
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                print(f"Response text: {response.text}")
            response.raise_for_status()
            data = response.json()

            # OHLC data: [[timestamp, open, high, low, close], ...]
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
            print(f"Raw OHLC data sample: {data[:2]}")
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df["volume"] = 0  # No volume from /ohlc
            print(f"Columns after fetch: {df.columns.tolist()}")
            print(f"Fetched {len(df)} price bars for {token_address} (4-hour intervals)")
            return PriceBar(token_address, base_currency, token_symbol, df)

        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            print(f"Price fetch failed: {e}, falling back to mock data")
            start_time = datetime.now() - timedelta(days=7)
            df = pd.DataFrame({
                "timestamp": pd.date_range(start=start_time, periods=42, freq="4h"),
                "open": [100] * 42,
                "high": [100] * 42,
                "low": [100] * 42,
                "close": [100] * 42,
                "volume": [0] * 42
            })
            print(f"Mock columns: {df.columns.tolist()}")
            return PriceBar(token_address, base_currency, token_symbol, df)