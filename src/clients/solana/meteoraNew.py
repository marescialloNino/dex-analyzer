import json
import requests
import subprocess  # For managing the Node.js server
import time
from typing import Dict, List, Optional
from solana.transaction import Transaction
from solders.pubkey import Pubkey
from .utils import convert_to_transaction
from .types import ActivationType, ActiveBin, FeeInfo, GetBins, GetPositionByUser, Position, PositionInfo, StrategyParameters, SwapQuote, LBPair, TokenReserve, DlmmHttpError as HTTPError

class MeteoraClient:
    """
    A Python client wrapper for the Meteora DLMM.
    """

    def __init__(self, rpc: str, pool_address: Pubkey, auto_start_server: bool = True):
        """
        Initializes the Meteora client.

        Args:
            rpc (str): The Solana RPC endpoint.
            pool_address (Pubkey): The public key of the DLMM pool.
            auto_start_server (bool, optional): Whether to automatically start the Node.js backend server. Defaults to True.
        """
        self.rpc = rpc
        self.pool_address = pool_address

        if auto_start_server:
            self._start_backend_server()

        # Give the server some time to start
        time.sleep(5)

        self.dlmm = DLMM_CLIENT.create(self.pool_address, self.rpc)

    def _start_backend_server(self):
        """
        Starts the Node.js backend server in a separate process.
        """
        try:
            #  Adjust the path to your ts-client directory
            self.backend_process = subprocess.Popen(
                ["npm", "run", "start-server"],
                cwd="ts-client",  #  Important: Set the working directory
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            print("Backend server started.")

        except Exception as e:
            raise Exception(f"Failed to start backend server: {e}")

    def stop_backend_server(self):
        """
        Stops the Node.js backend server.
        """
        if hasattr(self, "backend_process") and self.backend_process.poll() is None:
            self.backend_process.terminate()
            self.backend_process.wait()
            print("Backend server stopped.")

    def get_active_bin(self) -> ActiveBin:
        return self.dlmm.get_active_bin()

    def from_price_per_lamport(self, price: float) -> float:
        return self.dlmm.from_price_per_lamport(price)

    def to_price_per_lamport(self, price: float) -> float:
        return self.dlmm.to_price_per_lamport(price)

    def initialize_position_and_add_liquidity_by_strategy(self, position_pub_key: Pubkey, user: Pubkey, x_amount: int, y_amount: int, strategy: StrategyParameters) -> Transaction:
        return self.dlmm.initialize_position_and_add_liquidity_by_strategy(position_pub_key, user, x_amount, y_amount, strategy)

    def add_liquidity_by_strategy(self, position_pub_key: Pubkey, user: Pubkey, x_amount: int, y_amount: int, strategy: StrategyParameters) -> Transaction:
        return self.dlmm.add_liquidity_by_strategy(position_pub_key, user, x_amount, y_amount, strategy)

    def get_positions_by_user_and_lb_pair(self, user: Pubkey) -> GetPositionByUser:
        return self.dlmm.get_positions_by_user_and_lb_pair(user)

    def remove_liqidity(self, position_pub_key: Pubkey, user: Pubkey, bin_ids: List[int], bps: int, should_claim_and_close: bool) -> List[Transaction]:
        return self.dlmm.remove_liqidity(position_pub_key, user, bin_ids, bps, should_claim_and_close)

    def close_position(self, owner: Pubkey, position: Position) -> Transaction:
        return self.dlmm.close_position(owner, position)

    def get_bin_array_for_swap(self, swap_Y_to_X: bool, count: Optional[int]=4) -> List[dict]:
        return self.dlmm.get_bin_array_for_swap(swap_Y_to_X, count)

    def swap_quote(self, amount: int, swap_Y_to_X: bool, allowed_slippage: int, binArrays: List[dict], is_partial_filled: Optional[bool]=False) -> SwapQuote:
        return self.dlmm.swap_quote(amount, swap_Y_to_X, allowed_slippage, binArrays, is_partial_filled)

    def swap(self, in_token: Pubkey, out_token: Pubkey, in_amount: int, min_out_amount: int, lb_pair: Pubkey,  user: Pubkey, binArrays: List[Pubkey]) -> Transaction:
        return self.dlmm.swap(in_token, out_token, in_amount, min_out_amount, lb_pair, user, binArrays)

    def refetch_states(self) -> None:
        return self.dlmm.refetch_states()

    def get_bin_arrays(self) -> List[dict]:
        return self.dlmm.get_bin_arrays()

    def get_fee_info(self) -> FeeInfo:
        return self.dlmm.get_fee_info()

    def get_dynamic_fee(self) -> float:
        return self.dlmm.get_dynamic_fee()

    def get_bin_id_from_price(self, price: float, min: bool) -> int | None:
        return self.dlmm.get_bin_id_from_price(price, min)

    def get_bins_around_active_bin(self, number_of_bins_to_left: int, number_of_bins_to_right: int) -> GetBins:
        return self.dlmm.get_bins_around_active_bin(number_of_bins_to_left, number_of_bins_to_right)

    def get_bins_between_min_and_max_price(self, min_price: float, max_price: float) -> GetBins:
        return self.dlmm.get_bins_between_min_and_max_price(min_price, max_price)

    def get_bins_between_lower_and_upper_bound(self, lower_bound: int, upper_bound: int) -> GetBins:
        return self.dlmm.get_bins_between_lower_and_upper_bound(lower_bound, upper_bound)

    def claim_LM_reward(self, owner: Pubkey, position: Position) -> Transaction:
        return self.dlmm.claim_LM_reward(owner, position)

    def claim_all_LM_reards(self, owner: Pubkey, positions: List[Position]) -> List[Transaction]:
        return self.dlmm.claim_all_LM_reards(owner, positions)

    def claim_swap_fee(self, owner: Pubkey, position: Position) -> Transaction:
        return self.dlmm.claim_swap_fee(owner, position)

    def claim_all_swap_fees(self, owner: Pubkey, positions: List[Position]) -> List[Transaction]:
        return self.dlmm.claim_all_swap_fees(owner, positions)

    def claim_all_rewards(self, owner: Pubkey, positions: List[Position]) -> List[Transaction]:
        return self.dlmm.claim_all_rewards(owner, positions)

    def create_customizable_permissionless_lb_pair(self,
        bin_step: int,
        token_x: Pubkey,
        token_y: Pubkey,
        active_id: int,
        fee_bps: int,
        activation_type: int,
        has_alpha_vault: bool,
        creator_key: Pubkey,
        activation_point: Optional[int] = None
    ) -> Transaction:
        return DLMM_CLIENT.create_customizable_permissionless_lb_pair(
            bin_step,
            token_x,
            token_y,
            active_id,
            fee_bps,
            activation_type,
            has_alpha_vault,
            creator_key,
            activation_point
        )




