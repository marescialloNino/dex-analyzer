# src/models/price_bar.py
from dataclasses import dataclass
import pandas as pd

@dataclass
class PriceBar:
    token_address: str
    base_token: str
    data: pd.DataFrame  # Columns: ["bin_id", "price", "liquidity_x", "liquidity_y", "volume"]