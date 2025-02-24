# src/clients/solana/meteora.py
from typing import List, Optional
import requests
from solders.pubkey import Pubkey

from src.clients.base_client import BaseDEXClient
from src.models.pair import LiquidityPair
from src.models.position import Position
from src.models.price_bar import PriceBar
from src.utils.coingecko_price_fetcher import CoinGeckoPriceFetcher

class SolanaMeteoraClient(BaseDEXClient):
    def __init__(self, api_url: str = "https://dlmm-api.meteora.ag", rpc: str = "https://api.mainnet-beta.solana.com"):
        super().__init__("Solana", "Meteora")
        print(f"Setting API URL: {api_url}")
        self.api_url = api_url
        print(f"Setting RPC: {rpc}")
        self.rpc = rpc
        print("Creating session...")
        self.session = requests.Session()
        self.session.headers.update({
            'Content-type': 'application/json',
            'Accept': 'application/json',
            'rpc': rpc
        })
        print("Session headers set:", self.session.headers)
        self.stablecoins = {"USDC", "USDT"}
        self.pivots = {"SOL", "BTC"}
        self.price_fetcher = CoinGeckoPriceFetcher()

    def fetch_liquidity_pools(self) -> List[LiquidityPair]:
        print(f"Fetching pools from {self.api_url}/pair/all")
        try:
            response = self.session.get(f"{self.api_url}/pair/all", timeout=10)
            print(f"Status code: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                print(f"Unexpected response: {data}")
                return []
            print(f"Total pools in response: {len(data)}")
            if data:
                print(f"Sample pool: {data[0]}")
            pairs = []
            for pair in data:
                try:
                    name = pair.get("name", "")
                    mint_x = pair.get("mint_x", "")
                    mint_y = pair.get("mint_y", "")

                    name_parts = name.split("-")
                    if len(name_parts) == 2:
                        token0_symbol = name_parts[0]
                        token1_symbol = name_parts[1]
                    else:
                        print(f"Name split failed: {name}, using mint fallback")
                        token0_symbol = mint_x[-6:] if mint_x else "UNKNOWN"
                        token1_symbol = mint_y[-6:] if mint_y else "UNKNOWN"

                    if not mint_x or not mint_y:
                        print(f"Skipping pair with missing mints: {pair}")
                        continue

                    pairs.append(
                        LiquidityPair(
                            address=pair["address"],
                            token0_symbol=token0_symbol,
                            token0_address=mint_x,
                            token1_symbol=token1_symbol,
                            token1_address=mint_y,
                            tvl=float(pair.get("liquidity", 0)),
                            volume=float(pair.get("trade_volume_24h", 0))
                        )
                    )
                except (KeyError, ValueError) as e:
                    print(f"Skipping malformed pair: {pair}, Error: {e}")
            print(f"Processed pairs: {len(pairs)}")
            return pairs
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response text: {e.response.text}")
            return []

    def get_price_bars(self, token_address: str, base_token: str, days: int = 7, token_symbol: str = "UNKNOWN") -> PriceBar:
        return self.price_fetcher.get_price_bars(token_address, base_token, days, token_symbol)

    def get_open_positions(self, address: str) -> List[Position]:
        try:
            response = self.session.post(f"{self.api_url}/dlmm/get-all-lb-pair-positions-by-user", 
                                       data={"user": address}, timeout=10)
            response.raise_for_status()
            result = response.json()
            positions = []
            for lb_pair, pos_info in result.items():
                for pos in pos_info.get("userPositions", []):
                    positions.append(
                        Position(
                            address=pos.get("position", ""),
                            pool_address=lb_pair,
                            token0_amount=float(pos.get("totalXAmount", 0)),
                            token1_amount=float(pos.get("totalYAmount", 0)),
                            lower_bound=float(pos.get("lowerBinId", 0)),
                            upper_bound=float(pos.get("upperBinId", 0))
                        )
                    )
            return positions
        except requests.exceptions.RequestException as e:
            print(f"Positions fetch failed: {e}")
            return []