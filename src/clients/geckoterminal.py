import requests
import time
import datetime
from typing import Dict, List, Optional
import pandas as pd
from src.clients.base_client import BaseDEXClient
from src.models.pair import LiquidityPair
from src.models.price_bar import PriceBar
from src.constants import PIVOT_TOKENS, STABLECOINS

class GeckoTerminalClient(BaseDEXClient):
    def __init__(self, network: str, dex: str):
        """
        Initialize the GeckoTerminalClient.
        
        :param network: The network identifier (e.g., "solana")
        :param dex: The DEX identifier (e.g., "raydium")
        """
        super().__init__(chain_name=network, dex_name=dex)
        self.network = network
        self.dex = dex
        self.base_url = "https://api.geckoterminal.com/api/v2"
        self.session = requests.Session()
        self.rate_limit = 30  # calls per minute
        self.calls = []
        self.max_pages = 10  # Maximum pages allowed by the API
        
        # Use our constants for filtering
        self.pivots = PIVOT_TOKENS.get(network, set())
        self.stablecoins = STABLECOINS.get(network, set())

    def _rate_limit_check(self):
        """Enforce rate limiting: 30 calls per minute."""
        current_time = time.time()
        self.calls = [call for call in self.calls if current_time - call < 60]
        if len(self.calls) >= self.rate_limit:
            sleep_time = 60 - (current_time - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(current_time)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with rate limiting."""
        self._rate_limit_check()
        try:
            response = self.session.get(f"{self.base_url}{endpoint}", params=params)
            if response.status_code == 401:
                print(f"Error making request: 401 Unauthorized for url: {response.url}")
                return {"error": "Unauthorized"}
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            return {}

    def _get_all_pages(self, endpoint: str, params: Dict) -> List[Dict]:
        """Retrieve all available pages (up to max_pages) for an endpoint."""
        all_results = []
        for page in range(1, self.max_pages + 1):
            params['page'] = page
            response = self._make_request(endpoint, params)
            if not response or 'data' not in response or not response['data']:
                break
            all_results.extend(response['data'])
            time.sleep(0.2)  # 0.2-second delay between pages
        return all_results

    def fetch_liquidity_pools(
        self,
        all_pages: bool = True,
        min_tvl: float = 0.0,
        max_tvl: float = 10000000000.0,
        min_volume: float = 0.0,  
        no_pivots: bool = True,
        no_stables: bool = True,
        utility_pairs: bool = False
    ) -> List[LiquidityPair]:
        """
        Fetch liquidity pools from GeckoTerminal.
        
        If utility_pairs is True, only return pools where both tokens are pivot tokens.
        Otherwise, you can exclude pools that include pivot tokens (no_pivots) or stablecoins (no_stables).
        
        :param all_pages: If True, iterate through all available pages (up to max_pages).
        :param min_tvl: Only return pools with TVL >= min_tvl.
        :param min_volume: Only return pools with 24h volume >= min_volume (in USD).
        :param no_pivots: If True, exclude pools containing pivot tokens.
        :param no_stables: If True, exclude pools containing stablecoins.
        :param utility_pairs: If True, only include pools where both tokens are pivot tokens.
        :returns: A list of LiquidityPair objects.
        """
        params = {
            'include': 'base_token,quote_token',
            'sort': 'h24_volume_usd_desc'
        }
        if all_pages:
            pools_data = self._get_all_pages(f"/networks/{self.network}/dexes/{self.dex}/pools", params)
        else:
            params['page'] = 1
            result = self._make_request(f"/networks/{self.network}/dexes/{self.dex}/pools", params)
            pools_data = result.get('data', [])

        pairs = []
        for pool in pools_data:
            try:
                attributes = pool.get('attributes', {})
                relationships = pool.get('relationships', {})

                pool_address = attributes.get('address', '')
                name = attributes.get('name', '')
                tokens = [s.strip() for s in name.split('/') if s.strip()] if name else []
                if len(tokens) >= 2:
                    token0_symbol, token1_symbol = tokens[0], tokens[1]
                else:
                    token0_symbol = token1_symbol = "UNKNOWN"

                token0_address = relationships.get('base_token', {}).get('data', {}).get('id', '')
                token1_address = relationships.get('quote_token', {}).get('data', {}).get('id', '')

                try:
                    tvl = float(attributes.get('reserve_in_usd', 0))
                except Exception:
                    tvl = 0.0
                volume_dict = attributes.get('volume_usd', {})
                try:
                    volume = float(volume_dict.get('h24', 0))
                except Exception:
                    volume = 0.0

                # Apply filters for TVL and volume
                if tvl < min_tvl or volume < min_volume:
                    continue

                if utility_pairs:
                    # Only include pools where both tokens are pivot tokens.
                    if not (token0_symbol in self.pivots and token1_symbol in self.pivots):
                        continue
                else:
                    if no_pivots and (token0_symbol in self.pivots or token1_symbol in self.pivots):
                        continue
                    if no_stables and (token0_symbol in self.stablecoins or token1_symbol in self.stablecoins):
                        continue

                pair = LiquidityPair(
                    address=pool_address,
                    token0_symbol=token0_symbol,
                    token0_address=token0_address,
                    token1_symbol=token1_symbol,
                    token1_address=token1_address,
                    tvl=tvl,
                    volume=volume
                )
                pairs.append(pair)
            except Exception as e:
                print(f"Error processing pool data: {e}")
        return pairs

    def get_price_bars(
        self,
        pool_address: str,
        timeframe: str = "hour",
        aggregate: int = 1,
        limit: int = 1000,
        currency: str = "usd",
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        token: Optional[str] = None
    ) -> Optional[PriceBar]:
        """
        Fetch all OHLCV price bars for a given pool using GeckoTerminal's API, with pagination.
        
        :param pool_address: The pool's address.
        :param timeframe: Time interval (e.g., "hour", "day", "minute").
        :param aggregate: Aggregation level (day:{1}, hour:{1,4,12}, minute:{1,5,15}).
        :param limit: Number of price bars per request (max per call).
        :param currency: {"usd", "token"} Return OHLCV data in USD or in quote token.
        :param start_timestamp: Earliest timestamp to fetch from (seconds since epoch).
        :param end_timestamp: Latest timestamp to fetch up to (seconds since epoch).
        :param token: {"base", "quote"} Return OHLCV for base or quote token.
        :returns: A PriceBar object with all OHLCV data, or None if data isn't available.
        """
        endpoint = f"/networks/{self.network}/pools/{pool_address}/ohlcv/{timeframe}"
        params = {
            "aggregate": aggregate,
            "limit": limit,
            "currency": currency,
        }
        if end_timestamp is not None:
            params["before_timestamp"] = end_timestamp
        if token is not None:
            params["token"] = token

        all_ohlcv_list = []
        before_timestamp = end_timestamp

        while True:
            response = self._make_request(endpoint, params)
            if not response or "data" not in response or "attributes" not in response["data"]:
                print(f"No OHLCV data found for pool {pool_address} ({token or 'default'})")
                break

            attributes = response["data"]["attributes"]
            ohlcv_list = attributes.get("ohlcv_list", [])
            if not ohlcv_list:
                print(f"OHLCV list is empty for pool {pool_address} ({token or 'default'})")
                break

            all_ohlcv_list.extend(ohlcv_list)

            # Check if we need to fetch more data
            if len(ohlcv_list) == limit and (start_timestamp is None or 
                                            int(pd.to_datetime(ohlcv_list[-1][0], unit="s").timestamp()) > start_timestamp):
                earliest_timestamp = int(pd.to_datetime(ohlcv_list[-1][0], unit="s").timestamp())
                if start_timestamp and earliest_timestamp <= start_timestamp:
                    break  # Reached or passed the start timestamp
                before_timestamp = earliest_timestamp - 1
                params["before_timestamp"] = before_timestamp
                print(f"Hit limit ({limit} bars) for {pool_address} ({token or 'default'}). "
                      f"Fetching more before {datetime.datetime.fromtimestamp(before_timestamp)}")
                time.sleep(1)  # Small delay to respect rate limits
            else:
                break  # Less than limit returned or start_timestamp reached

        if not all_ohlcv_list:
            return None

        # Create DataFrame
        df = pd.DataFrame(all_ohlcv_list, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Filter by date range if specified
        if start_timestamp or end_timestamp:
            df = df[(df["timestamp"] >= pd.Timestamp(start_timestamp, unit="s") if start_timestamp else True) &
                    (df["timestamp"] <= pd.Timestamp(end_timestamp, unit="s") if end_timestamp else True)]

        meta = response.get("meta", {})
        base_symbol = meta.get("base", {}).get("symbol", "BASE")
        quote_symbol = meta.get("quote", {}).get("symbol", "QUOTE")

        return PriceBar(
            token_address=pool_address,
            base_token=base_symbol,
            token_symbol=quote_symbol,
            data=df
        )
    
    def get_open_positions(self, address: str):
        raise NotImplementedError("get_open_positions is not implemented for GeckoTerminalClient.")
