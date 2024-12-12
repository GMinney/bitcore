/* eslint-disable @typescript-eslint/no-explicit-any */
import { CryptoSearchInput, InputType } from './models';
import { getApiRoot } from './helper-methods';
import { fetcher } from '@/api/api';

const inputTypes: InputType[] = [
  // Standard THT / Legacy BCH address
  {
    regexes: [/^(thought:)?([43][a-km-zA-HJ-NP-Z1-9]{25,34})/],
    dataIndex: 2,
    type: 'address',
    chainNetworks: [{ chain: 'THT', network: 'mainnet' }],
  },
  // Standard THT / Legacy BCH address
  {
    regexes: [/^(thoughtcash:)?([43][a-km-zA-HJ-NP-Z1-9]{25,34})/],
    dataIndex: 2,
    type: 'address',
    chainNetworks: [{ chain: 'BCH', network: 'mainnet' }],
  },

  // Testnet THT Address
  {
    regexes: [/^(thought:)?([mt][1-9A-HJ-NP-Za-km-z]{26,35})/],
    dataIndex: 2,
    type: 'address',
    chainNetworks: [{ chain: 'THT', network: 'testnet' }],
  },
  // ETH Address
  {
    regexes: [/^0x[a-fA-F0-9]{40}$/],
    type: 'address',
    chainNetworks: [
      { chain: 'ETH', network: 'mainnet' },
      { chain: 'ETH', network: 'testnet' },
    ],
  },
  // THT block or tx
  {
    regexes: [/^[A-Fa-f0-9]{64}$/],
    type: 'blockOrTx',
    chainNetworks: [
      { chain: 'THT', network: 'mainnet' },
      { chain: 'THT', network: 'testnet' },
    ],
  },
  // ETH block or tx
  {
    regexes: [/^0x[A-Fa-f0-9]{64}$/],
    type: 'blockOrTx',
    chainNetworks: [
      { chain: 'ETH', network: 'mainnet' },
      { chain: 'ETH', network: 'testnet' },
    ],
  },
  // THT / ETH block height
  {
    regexes: [/^[0-9]{1,9}$/],
    type: 'block',
    chainNetworks: [
      { chain: 'THT', network: 'mainnet' },
      { chain: 'THT', network: 'testnet' },
    ],
  },
];

export const determineInputType = (input: string): Promise<CryptoSearchInput[]> => {
  const searchInputs: CryptoSearchInput[] = [];
  for (const { regexes, chainNetworks, type, dataIndex } of inputTypes) {
    const index = regexes.findIndex((regex) => regex.test(input));
    if (index > -1) {
      let localInput = input;
      // If defined then the data we care about is a subset of the actual user input (ie has prefix to discard)
      const matchInput = input.match(regexes[index]);

      if (dataIndex !== undefined && matchInput) {
        localInput = matchInput[dataIndex];
      }
      for (const chainNetwork of chainNetworks) {
        searchInputs.push({
          input: localInput,
          chainNetwork,
          type,
        });
      }
    }
  }
  return Promise.resolve(searchInputs);
};

export const searchValue = async (
  searchInputs: CryptoSearchInput[],
  currency: string | undefined,
  network: string | undefined,
): Promise<any> => {
  const searchArray: Array<Promise<any>> = [];
  if (currency && network) {
    // If user has selected a specific network, we only search that network for results
    searchInputs = searchInputs
      .filter((input) => input.chainNetwork.chain === currency)
      .filter((input) => input.chainNetwork.network === network);
  }

  for (const search of searchInputs) {
    const apiURL =
      getApiRoot(search.chainNetwork.chain) + `/${search.chainNetwork.chain}/${search.chainNetwork.network}`;
    if (search.type === 'block') {
      searchArray.push(searchBlock(search.input, apiURL));
    } else if (search.type === 'blockOrTx') {
      const block = searchBlock(search.input, apiURL);
      searchArray.push(block);
      const blockResult = await block;
      if (blockResult?.block) {
        break;
      }

      const tx = searchTx(search.input, apiURL);
      const result = await tx;
      searchArray.push(tx);
      if (result?.tx) {
        break;
      }
    } else if (search.type === 'address') {
      searchArray.push(searchAddress(search.input, apiURL));
    }
  }

  return Promise.all(searchArray);
};

const searchBlock = async (block: string, apiUrl: string): Promise<{ block: any }> => {
  try {
    const data = await fetcher(`${apiUrl}/block/${block}`);
    return Promise.resolve({ block: data });
  } catch (e: any) {
    return Promise.resolve(e.message);
  }
};

const searchTx = async (txid: string, apiUrl: string): Promise<{ tx: any }> => {
  try {
    const data = await fetcher(`${apiUrl}/tx/${txid}`);
    return Promise.resolve({ tx: data });
  } catch (e: any) {
    return Promise.resolve(e.message);
  }
};

const searchAddress = async (address: string, apiUrl: string): Promise<{ addr: any }> => {
  try {
    const data = await fetcher(`${apiUrl}/address/${address}/txs?limit=1`);
    return Promise.resolve({ addr: data });
  } catch (e: any) {
    return Promise.resolve(e.message);
  }
};
