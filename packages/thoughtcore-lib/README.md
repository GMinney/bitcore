# Thoughtcore JavaScript Library for Thought

[![NPM Package](https://img.shields.io/npm/v/thoughtcore-lib.svg?style=flat-square)](https://www.npmjs.org/package/thoughtcore-lib)
[![Build Status](https://img.shields.io/travis/thoughtnetwork/thoughtcore-lib.svg?branch=master&style=flat-square)](https://travis-ci.org/thoughtnetwork/thoughtcore-lib)
[![Coverage Status](https://img.shields.io/coveralls/thoughtnetwork/thoughtcore-lib.svg?style=flat-square)](https://coveralls.io/r/thoughtnetwork/thoughtcore-lib)

**A pure and powerful JavaScript library for Thought.**

## Principles

Thought is a powerful new peer-to-peer platform for the next generation of financial technology. The decentralized nature of the Thought network allows for highly resilient thought infrastructure, and the developer community needs reliable, open-source tools to implement thought apps and services. Thoughtcore JavaScript Library provides a reliable API for JavaScript apps that need to interface with Thought.

## Get Started

Clone the Thoughtcore monorepo and `npm install`:
```sh
git clone https://github.com/thoughtnetwork/thoughtcore.git
npm install
```
`cd` into thoughtcore-lib repository:
```sh
cd packages/thoughtcore-lib
```

## Building the Browser Bundle

To build a thoughtcore-lib full bundle for the browser:

```sh
gulp browser
```

This will generate files named `thoughtcore-lib.js` and `thoughtcore-lib.min.js`.

## Running Tests

```sh
npm test
```

You can also run just the Node.js tests with `gulp test:node`, just the browser tests with `gulp test:browser` or create a test coverage report (you can open `coverage/lcov-report/index.html` to visualize it) with `gulp coverage`.

## Documentation 

### Addresses and Key Management

- [Addresses](docs/address.md)
- [Using Different Networks](docs/networks.md)
- [Private Keys](docs/privatekey.md) and [Public Keys](docs/publickey.md)
- [Hierarchically-derived Private and Public Keys](docs/hierarchical.md)

### Payment Handling

- [Using Different Units](docs/unit.md)
- [Acknowledging and Requesting Payments: Thought URIs](docs/uri.md)
- [The Transaction Class](docs/transaction.md)
- [Unspent Transaction Output Class](docs/unspentoutput.md)

### Thought Internals

- [Scripts](docs/script.md)
- [Block](docs/block.md)

### Extra

- [Crypto](docs/crypto.md)
- [Encoding](docs/encoding.md)

### Module Development

- [Browser Builds](docs/browser.md)

### Modules

Some functionality is implemented as a module that can be installed separately:

- [Peer to Peer Networking](https://github.com/thoughtnetwork/thoughtcore/tree/master/packages/thoughtcore-p2p)
- [Thought Core JSON-RPC](https://github.com/thoughtnetwork/thoughtd-rpc)
- [Payment Channels](https://github.com/thoughtnetwork/thoughtcore-channel)
- [Mnemonics](https://github.com/thoughtnetwork/thoughtcore/tree/master/packages/thoughtcore-mnemonic)
- [Elliptical Curve Integrated Encryption Scheme](https://github.com/thoughtnetwork/thoughtcore-ecies)
- [Blockchain Explorers](https://github.com/thoughtnetwork/thoughtcore-explorers)
- [Signed Messages](https://github.com/thoughtnetwork/thoughtcore-message)

## Examples

- [Generate a random address](docs/examples.md#generate-a-random-address)
- [Generate a address from a SHA256 hash](docs/examples.md#generate-a-address-from-a-sha256-hash)
- [Import an address via WIF](docs/examples.md#import-an-address-via-wif)
- [Create a Transaction](docs/examples.md#create-a-transaction)
- [Sign a Thought message](docs/examples.md#sign-a-thought-message)
- [Verify a Thought message](docs/examples.md#verify-a-thought-message)
- [Create an OP RETURN transaction](docs/examples.md#create-an-op-return-transaction)
- [Create a 2-of-3 multisig P2SH address](docs/examples.md#create-a-2-of-3-multisig-p2sh-address)
- [Spend from a 2-of-2 multisig P2SH address](docs/examples.md#spend-from-a-2-of-2-multisig-p2sh-address)

## Security

We're using the Thoughtcore JavaScript Library in production, as are many others, but please use common sense when doing anything related to finances! We take no responsibility for your implementation decisions.

If you find a security issue, please email security@thoughtnetwork.com.

## Contributing

See [CONTRIBUTING.md](https://github.com/thoughtnetwork/thoughtcore/blob/master/Contributing.md) on the main thoughtcore repo for information about how to contribute.

## License

Code released under [the MIT license](https://github.com/thoughtnetwork/thoughtcore/blob/master/LICENSE).

Copyright 2013-2022 Thought, Inc. Thoughtcore is a trademark maintained by Thought, Inc.
