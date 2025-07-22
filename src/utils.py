# src/utils/utils.py
# Extracted from notebooks/gecko_terminal_example.ipynb and src/utils/analyzer.py

import os
from datetime import datetime
import pandas as pd
import numpy as np
from datetime import timedelta  # Added for Timedelta

def fetch_and_save_price_bars(client, pools, base_folder, start_timestamp, end_timestamp):
    """
    Fetch and save price bars for a list of pools into subfolders.
    
    :param client: GeckoTerminalClient instance.
    :param pools: List of LiquidityPair objects.
    :param base_folder: Base directory to save data.
    :param start_timestamp: Start timestamp for price bars.
    :param end_timestamp: End timestamp for price bars.
    :return: List of tuples (filename, DataFrame) for price data.
    """
    price_data_list = []
    for pool in pools:
        # Create a subfolder for the pool using token symbols joined by an underscore
        pool_folder = os.path.join(base_folder, f"{pool.token0_symbol}_{pool.token1_symbol}")
        os.makedirs(pool_folder, exist_ok=True)
        
        print(f"\nProcessing pool: {pool.token0_symbol}_{pool.token1_symbol} (Address: {pool.address}, Volume: ${pool.volume:,.2f})")
        
        # --- Base Token Processing ---
        base_filename = os.path.join(pool_folder, f"{pool.token0_symbol}_USD.csv")
        base_price_bar = client.get_price_bars(
            pool_address=pool.address,
            timeframe="hour",
            aggregate=1,
            limit=1000,
            currency="usd",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            token="base"
        )
        if base_price_bar and not base_price_bar.data.empty:
            base_df = base_price_bar.data
            base_df.to_csv(base_filename, index=False)
            print(f"Saved {len(base_df)} bars for {pool.token0_symbol} to {base_filename}")
            price_data_list.append((f"{pool.token0_symbol}_USD", base_df))
        else:
            print(f"No data for {pool.token0_symbol} in pool {pool.token0_symbol}_{pool.token1_symbol}")
        
        # --- Quote Token Processing ---
        quote_filename = os.path.join(pool_folder, f"{pool.token1_symbol}_USD.csv")
        quote_price_bar = client.get_price_bars(
            pool_address=pool.address,
            timeframe="hour",
            aggregate=1,
            limit=1000,
            currency="usd",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            token="quote"
        )
        if quote_price_bar and not quote_price_bar.data.empty:
            quote_df = quote_price_bar.data
            quote_df.to_csv(quote_filename, index=False)
            print(f"Saved {len(quote_df)} bars for {pool.token1_symbol} to {quote_filename}")
            price_data_list.append((f"{pool.token1_symbol}_USD", quote_df))
        else:
            print(f"No data for {pool.token1_symbol} in pool {pool.token0_symbol}_{pool.token1_symbol}")
        
        # --- Cross Token Processing ---
        # Use currency="token" to get the raw cross price data (typically relative to the quote token)
        cross_filename = os.path.join(pool_folder, f"{pool.token0_symbol}_{pool.token1_symbol}.csv")
        cross_price_bar = client.get_price_bars(
            pool_address=pool.address,
            timeframe="hour",
            aggregate=1,
            limit=1000,
            currency="token",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            token="quote"
        )
        if cross_price_bar and not cross_price_bar.data.empty:
            cross_df = cross_price_bar.data
            cross_df.to_csv(cross_filename, index=False)
            print(f"Saved {len(cross_df)} bars for {pool.token0_symbol}_{pool.token1_symbol} to {cross_filename}")
        else:
            print(f"No data for {pool.token0_symbol}_{pool.token1_symbol} in pool {pool.token0_symbol}_{pool.token1_symbol}")
    
    return price_data_list

def calculate_volume_and_fees_from_two(
    base_df: pd.DataFrame,
    tvl: float,
    fee_rate: float = 0.003
) -> dict:
    """
    Calculate the USD volume, estimated fees, and ratios for multiple periods: 3 days, 7 days, 14 days.
    
    Each DataFrame is assumed to contain:
      - 'timestamp': an ISO formatted datetime string (e.g., "2025-03-13 23:00:00").
      - 'volume': The traded volume in USD.
      
    The function will:
      1. Convert the timestamp columns to datetime.
      2. For each period (3, 7, 14 days), filter rows in that window from the latest timestamp.
      3. Sum the USD volumes to obtain the total volume for the period.
      4. Compute average 24hr volume as total_volume / days_in_period.
      5. Estimate fees as avg_daily_volume * fee_rate (for average daily fees).
      6. Compute avg_daily_fee-to-TVL and avg_daily_volume-to-TVL ratios.
    
    :param base_df: DataFrame from the base token (USD-valued).
    :param tvl: Total value locked in the pool.
    :param fee_rate: Fee rate for fee estimation (default is 0.003 for 0.3%).
    :return: Dictionary with volume and fee metrics for each period.
    """
    # Convert volume column to numeric in case it is read as string:
    base_df['volume'] = pd.to_numeric(base_df['volume'], errors='coerce')
    
    
    # Convert the timestamp columns to datetime (timestamps are ISO strings)
    if not pd.api.types.is_datetime64_any_dtype(base_df['timestamp']):
        base_df['timestamp'] = pd.to_datetime(base_df['timestamp'], errors='coerce')

        
    # Drop rows with missing key fields and sort:
    base_df = base_df.dropna(subset=["timestamp", "volume"]).sort_values("timestamp")
    
    # Determine the maximum timestamp available
    max_time = base_df['timestamp'].max() if not base_df.empty else pd.NaT
    
    metrics = {}
    for days_interval in [3, 7, 14]:  # Periods: 3 days, 1 week, 2 weeks
        if pd.isna(max_time):
            metrics.update({
                f"{days_interval}_days_avg_daily_volume": 0,
                f"{days_interval}_days_avg_daily_fees": 0,
                f"{days_interval}_days_avg_daily_fee_to_tvl_ratio": None,
                f"{days_interval}_days_avg_daily_volume_to_tvl_ratio": None
            })
            continue
        
        # Define the filtering window for the last `days_interval` days.
        days_interval_ago = max_time - timedelta(days=days_interval)
        
        interval_df = base_df[base_df['timestamp'] >= days_interval_ago].copy()
        
        print(f"DEBUG: Rows in data for last {days_interval} days = {len(interval_df)}")
        
        # Sum the volumes for the period
        total_volume = interval_df['volume'].sum()
        
        # Average daily volume
        avg_daily_volume = total_volume / days_interval if days_interval > 0 else 0
        
        # Average daily fees
        avg_daily_fees = avg_daily_volume * fee_rate
        
        # Ratios based on average daily values
        avg_daily_fee_to_tvl_ratio = avg_daily_fees / tvl if tvl > 0 else None
        avg_daily_volume_to_tvl_ratio = avg_daily_volume / tvl if tvl > 0 else None
        
        metrics.update({
            f"{days_interval}_days_avg_daily_volume": avg_daily_volume,
            f"{days_interval}_days_avg_daily_fees": avg_daily_fees,
            f"{days_interval}_days_avg_daily_fee_to_tvl_ratio": avg_daily_fee_to_tvl_ratio,
            f"{days_interval}_days_avg_daily_volume_to_tvl_ratio": avg_daily_volume_to_tvl_ratio
        })
    
    return metrics

def calculate_cross_price_volatility_from_df(cross_df: pd.DataFrame) -> dict:
    """
    Calculate the historical volatility for the cross price data over multiple periods: 3 days, 7 days, 14 days.
    
    The DataFrame is expected to contain a 'timestamp' column (with ISO formatted datetime strings
    such as "2025-03-13 23:00:00") and a 'close' column. If a 'price' column exists instead, it will be
    renamed to 'close'.
    
    The function performs the following for each period:
      1. Convert the 'timestamp' column to datetime.
      2. Filter the DataFrame to include only rows within the last X days (for X in [3, 7, 14]).
      3. Compute the log returns based on the 'close' price.
      4. Calculate historical volatility as the standard deviation of these log returns, annualized as std * sqrt(365 * 24) for hourly data.
    
    :param cross_df: DataFrame with historical cross price data.
    :return: Dictionary with keys like "3_days_volatility" containing the volatility values.
    """
    if 'timestamp' not in cross_df.columns:
        print("DataFrame missing 'timestamp' column.")
        return {}
    
    # Ensure there's a 'close' column; if not, try to rename 'price' to 'close'
    if 'close' not in cross_df.columns:
        if 'price' in cross_df.columns:
            cross_df = cross_df.rename(columns={'price': 'close'})
        else:
            print("DataFrame missing both 'close' and 'price' columns.")
            return {}
    
    # Convert 'timestamp' to datetime (assuming ISO formatted strings)
    if not pd.api.types.is_datetime64_any_dtype(cross_df['timestamp']):
        cross_df['timestamp'] = pd.to_datetime(cross_df['timestamp'], errors='coerce')
    
    # Drop rows missing required columns and sort by timestamp
    cross_df = cross_df.dropna(subset=['timestamp', 'close']).sort_values("timestamp")
    
    # Determine the maximum timestamp
    max_time = cross_df['timestamp'].max()
    
    metrics = {}
    for days_interval in [3, 7, 14]:  # Periods: 3 days, 7 days, 14 days
        if pd.isna(max_time):
            metrics[f"{days_interval}_days_volatility"] = np.nan
            continue
        
        # Filter the data to the last `days_interval` days
        start_time = max_time - timedelta(days=days_interval)
        filtered_df = cross_df[cross_df['timestamp'] >= start_time].copy()
        
        if filtered_df.empty or len(filtered_df) < 2:
            print(f"No sufficient data in the last {days_interval} days for volatility calculation.")
            metrics[f"{days_interval}_days_volatility"] = np.nan
            continue
        
        # Calculate log returns
        filtered_df['log_return'] = np.log(filtered_df['close'] / filtered_df['close'].shift(1))
        
        # Calculate historical volatility as the standard deviation of the log returns
        volatility = filtered_df['log_return'].std() * np.sqrt(365 * 24)  # Annualize the volatility (hourly data)
        
        metrics[f"{days_interval}_days_volatility"] = volatility
    
    return metrics

def calculate_pool_metrics(
    tvl: float,
    base_df: pd.DataFrame,
    cross_price_df: pd.DataFrame,
    fee_rate: float = 0.003
) -> dict:
    """
    Compute both the multi-period volume-based metrics and the multi-period volatility metrics.
    """
    # Calculate multi-period volume-based metrics using the base USD data.
    volume_metrics = calculate_volume_and_fees_from_two(base_df, tvl, fee_rate)
    # Calculate multi-period volatility metrics from the cross price data.
    volatility_metrics = calculate_cross_price_volatility_from_df(cross_price_df)
    
    # Combine results into a single dictionary.
    metrics = {**volume_metrics, **volatility_metrics}
    return metrics

def compute_correlation_matrix_from_dataframes(
    data_list: list[tuple[str, pd.DataFrame]]
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

def compute_correlation_matrix_from_pickle(pickle_files: list[str]) -> pd.DataFrame:
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
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
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
    
    return compute_correlation_matrix_from_dataframes(data_list)

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

def compute_beta_with_sol(sol_pickle: str, token_pickle_files: list[str]) -> dict[str, float]:
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
            
            beta = compute_beta_from_dataframes(token_df, sol_df)
            beta_results[token_symbol] = beta
        except Exception as e:
            print(f"[ERROR] Failed to compute beta for {file_path}: {e}")
    return beta_results