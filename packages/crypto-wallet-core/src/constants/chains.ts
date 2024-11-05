export const EVM_CHAIN_DEFAULT_TESTNET = {
  ETH: 'sepolia'
}

export const EVM_CHAIN_NETWORK_TO_CHAIN_ID = {
  // Mainnets
  ETH_mainnet: 1,
  // ETH testnets
  ETH_holesky: 17000,
  ETH_sepolia: 11155111,
  ETH_goerli: 5,
  ETH_kovan: 42,
  ETH_ropsten: 3,
  ETH_rinkeby: 4,

  // Regtests
  ETH_regtest: 1337,
}

const reverseObject = <K extends string, V extends number | string>(obj: Record<K, V>, split = false): Record<V, K> => {
  const reversed = {} as Record<V, K>;
  const entries = Object.entries(obj) as [K, V][];
  for (const [key, value] of entries) {
    const _key = split ? key.split('_')[0] as K : key;
    reversed[value] = _key;
  }
  return reversed;
};

export const EVM_CHAIN_ID_TO_CHAIN_NETWORK = reverseObject(EVM_CHAIN_NETWORK_TO_CHAIN_ID);

export const EVM_CHAIN_ID_TO_CHAIN = reverseObject(EVM_CHAIN_NETWORK_TO_CHAIN_ID, true);