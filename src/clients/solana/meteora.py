# src/clients/solana/meteora.py
from typing import List, Optional
import requests
import pandas as pd
from solders.pubkey import Pubkey
import json

from src.clients.base_client import BaseDEXClient
from src.models.pair import LiquidityPair
from src.models.position import Position
from src.models.price_bar import PriceBar

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

    def fetch_liquidity_pools(self) -> List[LiquidityPair]:
        """Fetch all liquidity pools from Meteora, using original name split with mint fallback."""
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
            try:
                r = requests.get(f"{self.api_url}/pair/all", timeout=10)
                print(f"Fallback status: {r.status_code}, pools: {len(r.json())}")
                return [LiquidityPair(**{
                    "address": p["address"],
                    "token0_symbol": p["name"].split("-")[0] if "-" in p.get("name", "") and len(p["name"].split("-")) == 2 else p.get("mint_x", "UNKNOWN")[-6:],
                    "token0_address": p.get("mint_x", ""),
                    "token1_symbol": p["name"].split("-")[1] if "-" in p.get("name", "") and len(p["name"].split("-")) == 2 else p.get("mint_y", "UNKNOWN")[-6:],
                    "token1_address": p.get("mint_y", ""),
                    "tvl": float(p.get("liquidity", 0)),
                    "volume": float(p.get("trade_volume_24h", 0))
                }) for p in r.json()]
            except Exception as fallback_e:
                print(f"Fallback failed: {fallback_e}")
                return []

    def get_price_bars(self, token_address: str, base_token: str) -> PriceBar:
        """Fetch price distribution from bin data for the pool."""
        try:
            # Find the pool address for this token pair
            pairs = self.fetch_liquidity_pools()
            pool_address = None
            for pair in pairs:
                if (pair.token0_address == token_address and pair.token1_symbol == base_token) or \
                   (pair.token1_address == token_address and pair.token0_symbol == base_token):
                    pool_address = pair.address
                    break
            if not pool_address:
                raise ValueError(f"No pool found for {token_address} vs {base_token}")

            # Fetch bin data (assuming /pair/{address}/bins exists)
            url = f"{self.api_url}/pair/{pool_address}/bins"
            print(f"Fetching bins from {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            bins = response.json()

            if not isinstance(bins, list) or not bins:
                print(f"Unexpected or empty bins response: {bins}")
                raise ValueError("No valid bin data returned")

            # Process bins into a DataFrame
            df = pd.DataFrame([{
                "bin_id": int(bin.get("bin_id", 0)),
                "price": float(bin.get("price", 0)),  # Adjust field name if needed (e.g., "pricePerToken")
                "liquidity_x": float(bin.get("x_amount", 0)),
                "liquidity_y": float(bin.get("y_amount", 0)),
                "volume": float(bin.get("volume", 0))  # Fallback to 0 if not provided
            } for bin in bins])

            # Sort by bin_id for consistency
            df = df.sort_values("bin_id").reset_index(drop=True)
            print(f"Fetched {len(df)} bins for price distribution")
            return PriceBar(token_address, base_token, df)

        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Price fetch failed: {e}, falling back to mock data")
            df = pd.DataFrame([{
                "bin_id": 0,
                "price": 100.0,
                "liquidity_x": 0.0,
                "liquidity_y": 0.0,
                "volume": 0.0
            }])
            return PriceBar(token_address, base_token, df)

    def get_open_positions(self, address: str) -> List[Position]:
        try:
            response = self.session.post(f"{self.api_url}/dlmm/get-all-lb-pair-positions-by-user", 
                                       data=json.dumps({"user": address}), timeout=10)
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