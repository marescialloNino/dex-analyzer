import pandas as pd
import numpy as np
import pickle
from typing import List

class Analyzer:
    @staticmethod
    def compute_correlation_matrix_from_dataframes(dfs: List[pd.DataFrame], tolerance="10min") -> pd.DataFrame:
        """
        Compute the correlation matrix from a list of DataFrames containing price bar data,
        aligning them using merge_asof on the timestamp column with the given tolerance.
        Each DataFrame must have a 'timestamp' and 'close' column.
        """
        if not dfs:
            return pd.DataFrame()

        # Prepare each DataFrame: ensure timestamps are tz-naive, sort, select columns, rename 'close'
        aligned_dfs = []
        for i, df in enumerate(dfs):
            df = df.copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"]).apply(lambda x: x.tz_localize(None) if x.tzinfo is not None else x)
            df = df.sort_values("timestamp")
            df = df[["timestamp", "close"]].reset_index(drop=True)
            df.rename(columns={"close": f"price_bar_{i}"}, inplace=True)
            aligned_dfs.append(df)
        
        # Use the first DataFrame as the base
        merged_df = aligned_dfs[0]
        # Iteratively merge subsequent DataFrames using merge_asof with the specified tolerance.
        for df in aligned_dfs[1:]:
            merged_df = pd.merge_asof(
                merged_df,
                df,
                on="timestamp",
                tolerance=pd.Timedelta(tolerance),
                direction="nearest"
            )
        
        # Drop rows with any missing values (i.e. where timestamps could not be aligned within tolerance)
        merged_df = merged_df.dropna()
        # Remove the timestamp column, leaving only the price series.
        combined = merged_df.drop(columns=["timestamp"])
        
        if combined.empty or combined.shape[0] < 2:
            print("Insufficient overlapping data for correlation.")
            return pd.DataFrame(np.nan, index=combined.columns, columns=combined.columns)
        
        print(f"Correlation matrix based on {merged_df.shape[0]} merged timestamps")
        return combined.corr()

    @staticmethod
    def compute_correlation_matrix_from_pickle(pickle_files: List[str], tolerance="10min") -> pd.DataFrame:
        """
        Load DataFrames from pickle files and compute their correlation matrix,
        aligning them using merge_asof with the given tolerance.
        
        Args:
            pickle_files (List[str]): List of file paths to pickled DataFrame objects.
            tolerance (str): Tolerance for merging timestamps (e.g., "10min").
            
        Returns:
            pd.DataFrame: Correlation matrix of the 'close' prices.
        """
        dataframes = []
        for file in pickle_files:
            try:
                with open(file, "rb") as f:
                    df = pickle.load(f)
                if isinstance(df, pd.DataFrame):
                    dataframes.append(df)
                else:
                    print(f"Object loaded from {file} is not a DataFrame.")
            except Exception as e:
                print(f"Failed to load object from {file}: {e}")
        if not dataframes:
            print("No DataFrames loaded from pickle files.")
            return pd.DataFrame()
        return Analyzer.compute_correlation_matrix_from_dataframes(dataframes, tolerance=tolerance)
