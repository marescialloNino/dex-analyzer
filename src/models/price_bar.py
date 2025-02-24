from dataclasses import dataclass
import pandas as pd

@dataclass
class PriceBar:
    token_address: str
    base_token: str
    token_symbol: str  
    data: pd.DataFrame  # Columns: ["timestamp", "open", "high", "low", "close", "volume"]