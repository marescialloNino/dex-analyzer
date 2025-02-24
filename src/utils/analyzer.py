import pandas as pd
from typing import List  # Add this import
from src.models.price_bar import PriceBar

class Analyzer:
    @staticmethod
    def compute_correlation_matrix(price_bars: List[PriceBar]) -> pd.DataFrame:
        combined = pd.concat([bar.data["price"] for bar in price_bars], axis=1)
        combined.columns = [f"{bar.token_address}_vs_{bar.base_token}" for bar in price_bars]
        return combined.corr()

    @staticmethod
    def compute_beta(token_prices: pd.DataFrame, chain_coin_prices: pd.DataFrame) -> float:
        returns_token = token_prices["price"].pct_change().dropna()
        returns_chain = chain_coin_prices["price"].pct_change().dropna()
        covariance = returns_token.cov(returns_chain)
        variance = returns_chain.var()
        return covariance / variance if variance != 0 else float('nan')