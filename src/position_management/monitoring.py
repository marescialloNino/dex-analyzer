from typing import List, Dict, Optional
from src.clients.solana.meteora import SolanaMeteoraClient
from src.models.position import Position
import pandas as pd

class PositionMonitor:
    def __init__(self, client: SolanaMeteoraClient):
        self.client = client
        self.positions_cache: Dict[str, List[Position]] = {}  # Cache by wallet address

    def get_open_positions(self, wallet_address: str) -> List[Position]:
        """Retrieve all open positions for a wallet address."""
        positions = self.client.get_open_positions(wallet_address)
        self.positions_cache[wallet_address] = positions
        print(f"Retrieved {len(positions)} open positions for wallet {wallet_address[:8]}...")
        return positions

    def get_position_details(self, position: Position) -> Dict:
        """Extract details for a single position."""
        details = {
            "pool_address": position.pool_address,
            "token0_amount": position.token0_amount,
            "token1_amount": position.token1_amount,
            "lower_bound": position.lower_bound,
            "upper_bound": position.upper_bound,
            "liquidity_profile": None  # Placeholder; Meteora API may not provide this directly
        }
        # Note: Liquidity profile (e.g., bins/distribution) requires additional API data or price context
        return details

    def monitor_positions(self, wallet_address: str) -> pd.DataFrame:
        """Monitor all positions for a wallet and return as DataFrame."""
        positions = self.get_open_positions(wallet_address)
        if not positions:
            print(f"No positions found for wallet {wallet_address[:8]}...")
            return pd.DataFrame()

        position_details = [self.get_position_details(pos) for pos in positions]
        df = pd.DataFrame(position_details)
        return df

    def check_price_bounds(self, position: Position, current_price: float) -> Optional[str]:
        """Check if current price is outside position bounds and return alert message if so."""
        if current_price < position.lower_bound:
            return f"Price {current_price:.4f} below lower bound {position.lower_bound:.4f} for pool {position.pool_address[:8]}..."
        elif current_price > position.upper_bound:
            return f"Price {current_price:.4f} above upper bound {position.upper_bound:.4f} for pool {position.pool_address[:8]}..."
        return None

    # Placeholder for Telegram alert (to be implemented later)
    def send_telegram_alert(self, message: str):
        print(f"[Telegram Alert Placeholder]: {message}")