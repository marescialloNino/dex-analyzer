# src/utils/analyzer.py
import pandas as pd
from typing import List
import numpy as np
import os

from src.models.price_bar import PriceBar

class Analyzer:
    @staticmethod
    def compute_correlation_matrix(price_bars: List[PriceBar]) -> pd.DataFrame:
        """Compute correlation matrix from a list of PriceBar objects using exact timestamp matches."""
        if not price_bars:
            print("No PriceBars provided for correlation")
            return pd.DataFrame()

        # Combine close prices into a single DataFrame with exact timestamp matches
        combined = pd.concat(
            [bar.data.set_index("timestamp")["close"].rename(f"{bar.token_symbol}") 
             for bar in price_bars],
            axis=1,
            join="inner"  # Only keep rows with matching timestamps across all PriceBars
        )

        # Check if there's enough data
        if combined.empty or combined.shape[0] < 2:
            print(f"Insufficient overlapping data for correlation (rows: {combined.shape[0]})")
            return pd.DataFrame(np.nan, index=combined.columns, columns=combined.columns)
        
        print(f"Correlation matrix based on {combined.shape[0]} overlapping timestamps")
        return combined.corr()
    
    def load_pickle_as_pricebar(pickle_path: str) -> PriceBar:
        """
        Reads a DataFrame from a pickle file and returns a PriceBar object.
        Assumes the pickle DataFrame has columns including ['timestamp', 'open', 'high', 'low', 'close', 'volume'].
        """
        # 1. Read the pickle into a DataFrame
        df = pd.read_pickle(pickle_path)
        
        # 2. Derive a token symbol from the filename (e.g. 'FWOG-USD.pkl' -> 'FWOG')
        base_name = os.path.splitext(os.path.basename(pickle_path))[0]  # 'FWOG-USD'
        token_symbol = base_name.rsplit("-", 1)[0]                      # 'FWOG'
        
        # 3. Create the PriceBar object
        price_bar = PriceBar(token_symbol=token_symbol, data=df)
        return price_bar

    @staticmethod
    def compute_beta(token_bar: PriceBar, chain_coin_bar: PriceBar) -> float:
        """Compute beta of token vs chain coin using exact timestamp matches."""
        token_data = token_bar.data.copy()
        chain_data = chain_coin_bar.data.copy()
        combined = pd.concat(
            [token_data.set_index("timestamp")["close"],
             chain_data.set_index("timestamp")["close"]],
            axis=1,
            join="inner"
        )
        combined.columns = ["token", "chain"]
        
        if combined.empty or combined.shape[0] < 2:
            print(f"Insufficient overlapping data for beta calculation between {token_bar.token_symbol} and {chain_coin_bar.token_symbol}")
            return float('nan')
        
        returns = combined.pct_change().dropna()
        covariance = returns["token"].cov(returns["chain"])
        variance = returns["chain"].var()
        return covariance / variance if variance != 0 else float('nan')