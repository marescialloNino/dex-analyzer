# Project: Solana Liquidity Pool Analysis

## Description
This project analyzes liquidity pool (LP) data from multiple decentralized exchanges on the Solana and Ethereum blockchains. It fetches LP data, processes price histories, computes correlations and beta values relative to native coin.

## Features
- Fetches liquidity pool data from GeckoTerminal API for DEXs (Meteora, Raydium, Orca, Uniswap V3, etc.).
- Extracts and saves price data for selected liquidity pools.
- Computes correlation matrices to identify relationships between assets.
- Computes beta values relative to native coin to assess asset volatility.
- Provides historical price data and visualizations.

## Installation
To use this project, ensure you have Python installed and then install the required dependencies:
```sh
pip install -r requirements.txt
```

## Usage
For example usage look into the gecko_terminal_example.ipynb notebook.