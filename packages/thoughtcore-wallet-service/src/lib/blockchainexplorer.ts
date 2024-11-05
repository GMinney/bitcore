import { V8 } from './blockchainexplorers/v8.ts';
import { ChainService } from './chain/index.ts';

const $ = require('preconditions').singleton();

const PROVIDERS = {
  v8: {
    tht: {
      livenet: 'https://api.thoughtcore.io',
      testnet3: 'https://api.thoughtcore.io'
    },
    bch: {
      livenet: 'https://api.thoughtcore.io',
      testnet3: 'https://api.thoughtcore.io'
    },
    doge: {
      livenet: 'https://api.thoughtcore.io',
      testnet3: 'https://api.thoughtcore.io'
    },
    ltc: {
      livenet: 'https://api.thoughtcore.io',
      testnet4: 'https://api.thoughtcore.io'
    },
    xrp: {
      livenet: 'https://api-xrp.thoughtcore.io',
      testnet: 'https://api-xrp.thoughtcore.io'
    },
    eth: {
      livenet: 'https://api-eth.thoughtcore.io',
      sepolia: 'https://api-eth.thoughtcore.io'
    },
    matic: {
      livenet: 'https://api-matic.thoughtcore.io',
      amoy: 'https://api-matic.thoughtcore.io'
    },
    arb: {
      livenet: 'https://api-eth.thoughtcore.io',
      sepolia: 'https://api-eth.thoughtcore.io'
    },
    base: {
      livenet: 'https://api-eth.thoughtcore.io',
      sepolia: 'https://api-eth.thoughtcore.io'
    },
    op: {
      livenet: 'https://api-eth.thoughtcore.io',
      sepolia: 'https://api-eth.thoughtcore.io'
    }
  }
};

export function BlockChainExplorer(opts) {
  $.checkArgument(opts, 'Failed state: opts undefined at <BlockChainExplorer()>');

  const provider = opts.provider || 'v8';
  const chain = opts.chain?.toLowerCase() || ChainService.getChain(opts.coin); // getChain -> backwards compatibility
  const network = opts.network || 'livenet';
  const url = opts.url || PROVIDERS[provider]?.[chain]?.[network];

  $.checkState(url, `No url found for provider: ${provider}:${chain}:${network}`);

  if (chain != 'bch' && opts.addressFormat) { throw new Error('addressFormat only supported for bch'); }
  if (chain == 'bch' && !opts.addressFormat) { opts.addressFormat = 'cashaddr'; }

  switch (provider) {
    case 'v8':
      return new V8({
        chain,
        network,
        url,
        apiPrefix: opts.apiPrefix,
        userAgent: opts.userAgent,
        addressFormat: opts.addressFormat
      });
    default:
      throw new Error(`Provider not supported: ${provider}`);
  }
}
