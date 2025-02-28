from enum import Enum

class Network(Enum):
    # networks ids taken from the geckoterminal networks list
    SOLANA = "solana"
    ETHEREUM = "eth"
    

class SolanaDex(Enum):
    RAYDIUM = "raydium"
    METEORA = "meteora"
    ORCA="orca"

class EthereumDex(Enum):
    UNISWAP_V3 = "uniswap_v3"
    UNISWAP_V2 = "uniswap_v2"
    

PIVOT_TOKENS = {
    Network.SOLANA.value: {"SOL", "JUP", "RAY", "JLP"},
    Network.ETHEREUM.value: {"ETH","WETH", "UNI"}
}


STABLECOINS = {
    Network.SOLANA.value: {"USDC", "USDT", "DAI", "USDE"},
    Network.ETHEREUM.value: {"USDC", "USDT", "DAI", "USDE"}
}
