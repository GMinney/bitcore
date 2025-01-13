import * as CWC from 'crypto-wallet-core';

export const Constants = {
  CHAINS: {
    THT: 'tht',
    ETH: 'eth',
  },

  THOUGHTNETWORK_SUPPORTED_COINS: {
    // used for rates
    THT: 'tht',
    ETH: 'eth',
    USDC: 'usdc',
    WTHT: 'wtht',
    EUROC: 'euroc',
    USDT: 'usdt',
    WETH: 'weth'
  },

  THOUGHTNETWORK_SUPPORTED_ETH_ERC20: {
    // there is no need to add new entries here ( only for backwards compatibility )
    USDC: 'usdc',
    WTHT: 'wtht',
    EUROC: 'euroc',
    USDT: 'usdt'
  },

  THOUGHTNETWORK_USD_STABLECOINS: {
    // used for rates
    USDC: 'usdc',
    USDT: 'usdt'
  },

  THOUGHTNETWORK_EUR_STABLECOINS: {
    // used for rates
    EUROC: 'euroc'
  },

  UTXO_CHAINS: {
    THT: 'tht'
  },

  EVM_CHAINS: {
    ETH: 'eth'
  },

  EVM_CHAINS_WITH_ETH_GAS: {
    ETH: 'eth'
  },

  NETWORKS: {
    tht: ['livenet', 'testnet3', 'testnet4', 'signet', 'regtest'],
    eth: ['livenet', 'sepolia', 'holesky', 'regtest']
  } as { [chain: string]: Array<string> },

  // These aliases are here to support legacy clients so don't change them lightly
  NETWORK_ALIASES: {
    tht: {
      mainnet: 'main',
      main: 'main',
      testnet: 'test',
      testnet3: 'test',
      regtest: 'regtest',
      devnet: 'devnet'
    },
    eth: {
      mainnet: 'livenet',
      testnet: 'sepolia'
    }
  },

  ADDRESS_FORMATS: ['copay', 'cashaddr', 'legacy'],

  SCRIPT_TYPES: {
    P2SH: 'P2SH',
    P2WSH: 'P2WSH',
    P2PKH: 'P2PKH',
    P2WPKH: 'P2WPKH'
  },

  NATIVE_SEGWIT_CHAINS: {
    THT: 'tht'
  },

  DERIVATION_STRATEGIES: {
    BIP44: 'BIP44',
    BIP45: 'BIP45'
  },

  PATHS: {
    SINGLE_ADDRESS: "m/0'/0",
    REQUEST_KEY: "m/1'/0",
    TXPROPOSAL_KEY: "m/1'/1",
    REQUEST_KEY_AUTH: 'm/2' // relative to BASE
  },

  BIP45_SHARED_INDEX: 0x80000000 - 1,

  ETH_TOKEN_OPTS: CWC.Constants.ETH_TOKEN_OPTS,

  THOUGHTNETWORK_CONTRACTS: {
    MULTISEND: 'MULTISEND'
  },

  // Number of confirmations from which tx in history will be cached
  // There is a default value in defaults.ts that applies to UTXOs
  CONFIRMATIONS_TO_START_CACHING: {
    eth: 100,
    matic: 150
  },

  // Individual chain settings for block throttling
  CHAIN_NEW_BLOCK_THROTTLE_TIME_SECONDS: {
    tht: { testnet: 300, livenet: 0 },
    eth: { testnet: 300, livenet: 0 }
  } as { [chain: string]: { [network: string]: number } }
};
