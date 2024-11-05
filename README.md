# Thoughtcore Monorepo

  <p align="center">
  <img alt="npm" src="https://img.shields.io/npm/v/thoughtcore-lib">
  <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/thoughtnetwork/thoughtcore">
  <a href="https://opensource.org/licenses/MIT/" target="_blank"><img alt="MIT License" src="https://img.shields.io/badge/License-MIT-blue.svg" style="display: inherit;"/></a>
  <img alt="GitHub contributors" src="https://img.shields.io/github/contributors/thoughtnetwork/thoughtcore">
  <br>
 <img src="https://circleci.com/gh/thoughtnetwork/thoughtcore.svg?style=shield" alt="master build">
</p>
  
**Infrastructure to build Thought and blockchain-based applications for the next generation of financial technology.**

## Insight Core
- [Thoughtcore Node](packages/thoughtcore-node) - A standardized API to interact with multiple blockchain networks
- [Insight](packages/insight) - A blockchain explorer web user interface

### Insight Core Dependents
- [Thoughtcore Wallet Client](packages/thoughtcore-wallet-client) - A client for the wallet service
- [Thoughtcore Lib](packages/thoughtcore-lib) - A powerful JavaScript library for Thought
- [Thoughtcore P2P](packages/thoughtcore-p2p) - The peer-to-peer networking protocol for Thought
- [Thoughtcore Client](packages/thoughtcore-client) - A helper to create a wallet using the thoughtcore-v8 infrastructure
- [Crypto Wallet Core](packages/crypto-wallet-core) - A coin-agnostic wallet library for creating transactions, signing, and address derivation
- [Crypto RPC](packages/crypto-rpc) - RPC wrapper for multiple rpcs
- - [Thoughtd RPC](packages/crypto-rpc) - A client library to connect to Thought Core RPC in JavaScript.


## Setup
- Ports 
  - Insight and thoughtcore-node api - 3000 for public web applications,  
  - Thought Network - 10617 (rpc) 10618 (peers) 11617 (rpc testnet) 11618 (peers testnet)

## Applications

- [Thoughtcore Node](packages/thoughtcore-node) - A standardized API to interact with multiple blockchain networks
- [Thoughtcore Wallet](packages/thoughtcore-wallet) - A command-line based wallet client
- [Thoughtcore Wallet Client](packages/thoughtcore-wallet-client) - A client for the wallet service
- [Thoughtcore Wallet Service](packages/thoughtcore-wallet-service) - A multisig HD service for wallets
- [Thoughtnetwork Wallet](https://github.com/thoughtnetwork/wallet) - An easy-to-use, multiplatform, multisignature, secure thought wallet
- [Insight](packages/insight) - A blockchain explorer web user interface

## Libraries

- [Thoughtcore Lib](packages/thoughtcore-lib) - A powerful JavaScript library for Thought
- [Thoughtcore Mnemonic](packages/thoughtcore-mnemonic) - Implements mnemonic code for generating deterministic keys
- [Thoughtcore P2P](packages/thoughtcore-p2p) - The peer-to-peer networking protocol for Thought
- [Crypto Wallet Core](packages/crypto-wallet-core) - A coin-agnostic wallet library for creating transactions, signing, and address derivation

## Extras

- [Thoughtcore Build](packages/thoughtcore-build) - A helper to add tasks to gulp
- [Thoughtcore Client](packages/thoughtcore-client) - A helper to create a wallet using the thoughtcore-v8 infrastructure

## Contributing

See [CONTRIBUTING.md](https://github.com/thoughtnetwork/thoughtcore/blob/master/Contributing.md) on the main thoughtcore repo for information about how to contribute.

## License

Code released under [the MIT license](https://github.com/thoughtnetwork/thoughtcore/blob/master/LICENSE).

Copyright 2013-2023 Thought, Inc. Thoughtcore is a trademark maintained by Thought, Inc.
