# src/utils/analyzer.py
import pandas as pd
from typing import List
import numpy as np

from src.models.price_bar import PriceBar

class Analyzer:
    @staticmethod
    def compute_correlation_matrix(price_bars: List[PriceBar]) -> pd.DataFrame:
        """Compute correlation matrix from a list of PriceBar objects using close prices with ±10-minute window."""
        # Create a common 10-minute interval timeline starting from the earliest timestamp
        all_timestamps = sorted(set().union(*[set(bar.data["timestamp"]) for bar in price_bars]))
        if not all_timestamps:
            print("No timestamps available for correlation")
            return pd.DataFrame()

        min_time = min(all_timestamps)
        max_time = max(all_timestamps)
        common_index = pd.date_range(
            start=min_time.floor("10min"),
            end=max_time.ceil("10min"),
            freq="10min"
        )

        # Combine close prices with a 10-minute tolerance
        combined = pd.DataFrame(index=common_index)
        for bar in price_bars:
            # Floor timestamps to nearest 10 minutes
            bar_data = bar.data.copy()
            bar_data["timestamp_bin"] = pd.to_datetime(bar_data["timestamp"]).dt.floor("10min")
            # Merge with common index, forward-fill within 10 minutes
            temp = bar_data.set_index("timestamp_bin")["close"].reindex(common_index, method="ffill", limit=1)
            combined[f"{bar.token_symbol}_vs_{bar.base_token}"] = temp
        
        # Drop rows with any NaN (no data within 10 minutes)
        combined = combined.dropna()
        
        # Check if there's enough data
        if combined.empty or combined.shape[0] < 2:
            print(f"Insufficient overlapping data for correlation with ±10-minute window (rows: {combined.shape[0]})")
            return pd.DataFrame(np.nan, index=combined.columns, columns=combined.columns)
        
        print(f"Correlation matrix based on {combined.shape[0]} overlapping timestamps")
        return combined.corr()

    @staticmethod
    def compute_beta(token_bar: PriceBar, chain_coin_bar: PriceBar) -> float:
        """Compute beta of token vs chain coin (e.g., SOL) using close prices with ±10-minute window."""
        # Floor timestamps to nearest 10 minutes
        token_data = token_bar.data.copy()
        chain_data = chain_coin_bar.data.copy()
        token_data["timestamp_bin"] = pd.to_datetime(token_data["timestamp"]).dt.floor("10min")
        chain_data["timestamp_bin"] = pd.to_datetime(chain_data["timestamp"]).dt.floor("10min")
        
        # Create a common 10-minute index
        common_index = pd.date_range(
            start=min(token_data["timestamp_bin"].min(), chain_data["timestamp_bin"].min()),
            end=max(token_data["timestamp_bin"].max(), chain_data["timestamp_bin"].max()),
            freq="10min"
        )
        
        # Reindex and forward-fill within 10 minutes
        token_prices = token_data.set_index("timestamp_bin")["close"].reindex(common_index, method="ffill", limit=1)
        chain_prices = chain_data.set_index("timestamp_bin")["close"].reindex(common_index, method="ffill", limit=1)
        
        combined = pd.concat([token_prices, chain_prices], axis=1, join="inner")
        combined.columns = ["token", "chain"]
        
        if combined.empty or combined.shape[0] < 2:
            print(f"Insufficient overlapping data for beta calculation between {token_bar.token_symbol} and {chain_coin_bar.token_symbol}")
            return float('nan')
        
        returns = combined.pct_change().dropna()
        covariance = returns["token"].cov(returns["chain"])
        variance = returns["chain"].var()
        
        return covariance / variance if variance != 0 else float('nan')