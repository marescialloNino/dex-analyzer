from dataclasses import dataclass
from typing import Optional  # Add this import

@dataclass
class Position:
    position_id: str
    pair_id: str
    token0_qty: float
    token1_qty: float
    lower_bound: Optional[float] = None  # Optional for now; Meteora may not provide this
    upper_bound: Optional[float] = None  # Optional for now
