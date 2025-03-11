# src/clients/solana/meteora.py
from typing import List, Optional, Dict
import requests
from solders.pubkey import Pubkey

from src.clients.base_client import BaseDEXClient
from src.models.pair import LiquidityPair
from src.models.position import Position
from src.models.price_bar import PriceBar
from src.utils.moralis_price_fetcher import MoralisPriceFetcher
import pandas as pd

class SolanaMeteoraClient(BaseDEXClient):
    SOL_ADDRESS = "So11111111111111111111111111111111111111112"
    USDC_ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    UTILITY_TOKENS = {
        "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4": "JLP",
        "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": "JUP",
        "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R": "RAY"
    }

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
        self.price_fetcher = MoralisPriceFetcher()
        self.token_symbols = self._fetch_token_symbols()

    def _fetch_token_symbols(self) -> Dict[str, str]:
        """Fetch token address to symbol mapping from CoinGecko."""
        print("Fetching token symbols from CoinGecko...")
        try:
            url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            coins = response.json()
            symbol_map = {}
            for coin in coins:
                platforms = coin.get("platforms", {})
                solana_mint = platforms.get("solana", "")
                if solana_mint:
                    symbol_map[solana_mint] = coin["symbol"].upper()
            print(f"Mapped {len(symbol_map)} Solana token addresses to symbols")
            return symbol_map
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch token symbols from CoinGecko: {e}")
            return {
                self.SOL_ADDRESS: "SOL",
                self.USDC_ADDRESS: "USDC",
                **self.UTILITY_TOKENS
            }

    def fetch_liquidity_pools(self) -> List[LiquidityPair]:
        """Fetch all liquidity pools from Meteora."""
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
                    mint_x = pair.get("mint_x", "")
                    mint_y = pair.get("mint_y", "")
                    if not mint_x or not mint_y:
                        print(f"Skipping pair with missing mints: {pair}")
                        continue

                    token0_symbol = self.token_symbols.get(mint_x, mint_x[-6:] if mint_x else "UNKNOWN")
                    token1_symbol = self.token_symbols.get(mint_y, mint_y[-6:] if mint_y else "UNKNOWN")

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

    def get_pools_for_token(self, token_address: str, pools: List[LiquidityPair]) -> List[LiquidityPair]:
        """Find all pools containing a specific token address."""
        return [
            pool for pool in pools
            if pool.token0_address == token_address or pool.token1_address == token_address
        ]

    def get_largest_pool(self, token_address: str, pools: List[LiquidityPair], base_token: Optional[str] = None) -> Optional[LiquidityPair]:
        """Find the largest pool (by TVL) for a token, optionally filtered by base token."""
        token_pools = self.get_pools_for_token(token_address, pools)
        if not token_pools:
            return None
        if base_token:
            token_pools = [p for p in token_pools if p.token0_address == base_token or p.token1_address == base_token]
        return max(token_pools, key=lambda p: p.tvl, default=None)

    def filter_native_utility_pairs(self, pools: List[LiquidityPair]) -> List[LiquidityPair]:
        """Filter pools that pair the native token (SOL) with a DEX/L2 utility token."""
        return [
            pool for pool in pools
            if (pool.token0_address == self.SOL_ADDRESS and pool.token1_address in self.UTILITY_TOKENS) or
               (pool.token1_address == self.SOL_ADDRESS and pool.token0_address in self.UTILITY_TOKENS)
        ]

    def get_price_bars(self, pair_address: str, token_symbol: str, from_date: str, to_date: str, timeframe: str = "1h", base_currency: str = "usd") -> PriceBar:
        """Fetch price bars for a specific pair address using Moralis."""
        return self.price_fetcher.get_price_bars(pair_address, token_symbol, from_date, to_date, timeframe, base_currency)
  
    
    def get_all_positions_for_user(self, user_pubkey: str) -> Dict[str, List[Position]]:
        """
        Retrieves all open positions for a given user across all Meteora DLMM pools.
        Args:
            user_pubkey (str): The public key of the user as a string.
        Returns:
            Dict[str, List[Position]]: Dictionary with pool addresses as keys and lists of Position objects as values.
        """
        print(f"Fetching all positions for user {user_pubkey[:8]}...")
        try:
            response = self.session.post(
                f"{self.api_url}/dlmm/get-all-lb-pair-positions-by-user",
                json={"user": user_pubkey},
                timeout=10
            )
            print(f"Status code: {response.status_code}")
            response.raise_for_status()
            result = response.json()

            positions_by_pool = {}
            for pool_address, pos_info in result.items():
                positions = []
                token_x = pos_info.get("tokenX", {}).get("mint", "UNKNOWN")
                token_y = pos_info.get("tokenY", {}).get("mint", "UNKNOWN")
                for pos in pos_info.get("userPositions", []):
                    positions.append(
                        Position(
                            address=pos.get("position", ""),
                            pool_address=pool_address,
                            token0_amount=float(pos.get("totalXAmount", 0)),
                            token1_amount=float(pos.get("totalYAmount", 0)),
                            lower_bound=float(pos.get("lowerBinId", 0)),
                            upper_bound=float(pos.get("upperBinId", 0)),
                            token_x=token_x,
                            token_y=token_y
                        )
                    )
                if positions:
                    positions_by_pool[pool_address] = positions
            print(f"Retrieved positions for {len(positions_by_pool)} pools")
            return positions_by_pool
        except requests.exceptions.RequestException as e:
            print(f"Positions fetch failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response text: {e.response.text}")
            return {}
