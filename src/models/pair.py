from dataclasses import dataclass

@dataclass
class LiquidityPair:
    address: str
    token0_symbol: str
    token0_address: str  
    token1_symbol: str
    token1_address: str  
    tvl: float
    volume: float
