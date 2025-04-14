from enum import Enum

class Network(Enum):
    # networks ids taken from the geckoterminal networks list
    SOLANA = "solana"
    ETHEREUM = "eth"
    BSC= "bsc"
    POLYGONPOS = "polygon_pos"
    ARBITRUM = "arbitrum"
    SUI = "sui-network"
    BASE= "base"

    
NETWORK_CONFIG = {
    Network.SOLANA.value: {
        "dexes": ["raydium", "meteora", "orca"],
        "pivot_tokens": {"SOL", "JUP", "RAY", "JLP", "JupSOL", "JitoSOL", "dSOL"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.ETHEREUM.value: {
        "dexes": ["uniswap_v3", "uniswap_v2"],
        "pivot_tokens": {"ETH", "WETH", "UNI"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.BSC.value: {
        "dexes": ["pancakeswap-v3-bsc", "traderjoe-v2-1-bsc"],
        "pivot_tokens": {"BNB", "CAKE"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.POLYGONPOS.value: {
        "dexes": ["quickswap_v3"],
        "pivot_tokens": {"POL", "ETH", "WETH"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.ARBITRUM.value: {
        "dexes": ["camelot","traderjoe-v2-arbitrum"],
        "pivot_tokens": {"ARB", "ETH", "WETH"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.SUI.value: {
        "dexes": ["kriya-dex"],
        "pivot_tokens": {"SUI"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
    Network.BASE.value: {
        "dexes": ["aerodrome-base", "uniswap-v3-base"],
        "pivot_tokens": {"ETH", "WETH"},
        "stables": {"USDC", "USDT", "DAI", "USDE"}
    },
}