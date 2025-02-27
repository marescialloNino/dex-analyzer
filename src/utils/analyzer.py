import pandas as pd
import numpy as np
import os
from typing import List, Tuple, Dict

class Analyzer:
    @staticmethod
    def compute_correlation_matrix_from_dataframes(
        data_list: List[Tuple[str, pd.DataFrame]]
    ) -> pd.DataFrame:
        """
        Compute a correlation matrix from a list of tuples (token_symbol, DataFrame),
        where each DataFrame contains at least 'timestamp' and 'close' columns.
        The DataFrames are aligned using exact timestamp matches (inner join).
        """
        if not data_list:
            print("No DataFrames provided for correlation.")
            return pd.DataFrame()
        
        # Rename each DataFrame's 'close' column using the token symbol,
        # then align all DataFrames on their timestamps.
        combined = pd.concat(
            [df.set_index("timestamp")["close"].rename(token_symbol)
             for token_symbol, df in data_list],
            axis=1,
            join="inner"
        )
        
        if combined.empty or combined.shape[0] < 2:
            print(f"Insufficient overlapping data for correlation (rows: {combined.shape[0]})")
            return pd.DataFrame(np.nan, index=combined.columns, columns=combined.columns)
        
        print(f"Correlation matrix based on {combined.shape[0]} overlapping timestamps")
        return combined.corr()

    @staticmethod
    def compute_correlation_matrix_from_pickle(pickle_files: List[str]) -> pd.DataFrame:
        """
        Load DataFrames from pickle files, extract the token symbol from the filename,
        and compute their correlation matrix by reusing compute_correlation_matrix_from_dataframes.
        
        Each pickle file is assumed to contain a DataFrame with at least
        'timestamp' and 'close' columns.
        """
        data_list = []
        for file_path in pickle_files:
            try:
                # Load the DataFrame from the pickle
                df = pd.read_pickle(file_path)
                if not isinstance(df, pd.DataFrame):
                    print(f"[WARNING] Object loaded from {file_path} is not a DataFrame. Skipping.")
                    continue
                
                # Ensure the 'timestamp' column is converted to datetime
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df.dropna(subset=["timestamp", "close"], inplace=True)
                
                # Parse the token symbol from the filename.
                # For example, 'FWOG-USD.pkl' will yield token_symbol 'FWOG'
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                token_symbol = base_name.rsplit("-", 1)[0]
                
                data_list.append((token_symbol, df))
            except Exception as e:
                print(f"[ERROR] Failed to load {file_path}: {e}")
        
        if not data_list:
            print("No valid DataFrames loaded from pickle files.")
            return pd.DataFrame()
        
        return Analyzer.compute_correlation_matrix_from_dataframes(data_list)
    
    @staticmethod
    def compute_beta_from_dataframes(token_df: pd.DataFrame, sol_df: pd.DataFrame) -> float:
        """
        Compute the beta of a token (given its DataFrame) relative to SOL (given its DataFrame).
        The DataFrames must include 'timestamp' and 'close' columns.
        """
        # Ensure timestamps are datetime objects
        token_df["timestamp"] = pd.to_datetime(token_df["timestamp"], errors="coerce")
        sol_df["timestamp"] = pd.to_datetime(sol_df["timestamp"], errors="coerce")
        
        # Drop rows with missing 'timestamp' or 'close'
        token_df = token_df.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
        sol_df = sol_df.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
        
        # Set index to timestamp for both series
        token_series = token_df.set_index("timestamp")["close"]
        sol_series = sol_df.set_index("timestamp")["close"]
        
        # Merge the two series on their timestamps (inner join keeps only overlapping timestamps)
        combined = pd.concat([token_series, sol_series], axis=1, join="inner")
        combined.columns = ["token", "sol"]
        
        if combined.empty or combined.shape[0] < 2:
            print("Insufficient overlapping data for beta calculation.")
            return float("nan")
        
        # Compute returns (percentage change)
        returns = combined.pct_change().dropna()
        covariance = returns["token"].cov(returns["sol"])
        variance = returns["sol"].var()
        beta = covariance / variance if variance != 0 else float("nan")
        return beta
    
    @staticmethod
    def compute_beta_with_sol(sol_pickle: str, token_pickle_files: List[str]) -> Dict[str, float]:
        """
        Compute the beta of each token relative to SOL.
        
        Args:
            sol_pickle (str): Path to the SOL pickle file (DataFrame with 'timestamp' and 'close').
            token_pickle_files (List[str]): List of paths to token pickle files.
        
        Returns:
            Dict[str, float]: A dictionary mapping each token symbol (extracted from the filename)
                              to its beta relative to SOL.
        """
        try:
            sol_df = pd.read_pickle(sol_pickle)
            # Ensure SOL DataFrame is clean
            sol_df["timestamp"] = pd.to_datetime(sol_df["timestamp"], errors="coerce")
            sol_df = sol_df.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
        except Exception as e:
            print(f"[ERROR] Failed to load SOL pickle file '{sol_pickle}': {e}")
            return {}
        
        beta_results = {}
        for file_path in token_pickle_files:
            try:
                # Extract token symbol from filename, e.g. 'FWOG-USD.pkl' -> 'FWOG'
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                token_symbol = base_name.rsplit("-", 1)[0]
                
                token_df = pd.read_pickle(file_path)
                token_df["timestamp"] = pd.to_datetime(token_df["timestamp"], errors="coerce")
                token_df = token_df.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
                
                beta = Analyzer.compute_beta_from_dataframes(token_df, sol_df)
                beta_results[token_symbol] = beta
            except Exception as e:
                print(f"[ERROR] Failed to compute beta for {file_path}: {e}")
        return beta_results
