# src/clients/base_client.py
from abc import ABC, abstractmethod
from typing import List
from src.models.pair import LiquidityPair
from src.models.position import Position
from src.models.price_bar import PriceBar

class BaseDEXClient(ABC):
    def __init__(self, chain_name: str, dex_name: str):
        self.chain_name = chain_name
        self.dex_name = dex_name
        self.stablecoins = {"USDC", "USDT", "DAI"}
        self.pivots = {"SOL", "RAY", "JLP", "JUP"} 

    @abstractmethod
    def fetch_liquidity_pools(self) -> List[LiquidityPair]:
        """Fetch all liquidity pools."""
        pass

    @abstractmethod
    def get_price_bars(self, token_address: str, base_token: str, depth: str, period: str) -> PriceBar:
        """Fetch price bars."""
        pass


    def filter_pairs(self, pairs: List[LiquidityPair], min_tvl: float = 10000, min_volume: float = 5000, no_stables: bool=True, no_pivots: bool=True) -> List[LiquidityPair]:
        """Filter pairs."""
        if (no_stables and no_pivots):
            return [
                pair for pair in pairs
                if (pair.token0_symbol not in self.stablecoins and 
                    pair.token1_symbol not in self.stablecoins and 
                    pair.token0_symbol not in self.pivots and 
                    pair.token1_symbol not in self.pivots and 
                    pair.tvl >= min_tvl and pair.volume >= min_volume)
            ]
        elif (no_stables and not no_pivots):
            return [
                pair for pair in pairs
                if (pair.token0_symbol not in self.stablecoins and 
                    pair.token1_symbol not in self.stablecoins and  
                    pair.tvl >= min_tvl and pair.volume >= min_volume)
            ]
        elif (no_pivots and not no_stables):
            return [
                pair for pair in pairs
                if (pair.token0_symbol not in self.stablecoins and 
                    pair.token1_symbol not in self.stablecoins and 
                    pair.tvl >= min_tvl and pair.volume >= min_volume)
            ]
        else:
            return [
                pair for pair in pairs
                if (pair.tvl >= min_tvl and pair.volume >= min_volume)
            ]