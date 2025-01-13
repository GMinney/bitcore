export type NetworkType = 'main' | 'test' | 'regtest';

export interface Chain {
  chain: string;
}
export interface Network {
  network: string; // TODO change this to NetworkType
}
export type ChainNetwork = Chain & Network;

export interface ChainId { chainId: string | number }
