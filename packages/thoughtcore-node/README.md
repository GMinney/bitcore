# Thoughtcore Node API

**A standardized API to interact with the Thought blockchain network**

Currently supporting:
**[Thought](https://thoughtai.org/), [Thought](https://thought.live/)**


## Getting Started

### Requirements

- Trusted P2P Client with an open RPC endpoint
- MongoDB Server >= v3.4
- make g++ gcc 

### Checkout the repo


```sh
git clone git@github.com:thoughtnetwork/thoughtcore.git
git checkout master
npm install
```

## Setup Guide

### 1. Setup Thoughtcore config

The definition for all the chain configuration can be found in `src/types/Config.ts`

<details>
<summary>Example thoughtcore.config.json</summary>
<br>

```json
{
  "thoughtcoreNode": {
    "chains": {
      "THT": {
        "mainnet": {
          "chainSource": "p2p",
          "trustedPeers": [
            {
              "host": "127.0.0.1",
              "port": 20008
            }
          ],
          "rpc": {
            "host": "127.0.0.1",
            "port": 20009,
            "username": "username",
            "password": "password"
          }
        },
        "regtest": {
          "chainSource": "p2p",
          "trustedPeers": [
            {
              "host": "127.0.0.1",
              "port": 20020
            }
          ],
          "rpc": {
            "host": "127.0.0.1",
            "port": 20021,
            "username": "username",
            "password": "password"
          }
        }
      },
      "BCH": {
        "mainnet": {
          "parentChain": "THT",
          "forkHeight": 478558,
          "trustedPeers": [
            {
              "host": "127.0.0.1",
              "port": 30008
            }
          ],
          "rpc": {
            "host": "127.0.0.1",
            "port": 30009,
            "username": "username",
            "password": "password"
          }
        },
        "regtest": {
          "chainSource": "p2p",
          "trustedPeers": [
            {
              "host": "127.0.0.1",
              "port": 30020
            }
          ],
          "rpc": {
            "host": "127.0.0.1",
            "port": 30021,
            "username": "username",
            "password": "password"
          }
        }
      }
    }
  }
}
```

</details>

### 2. Setup Your Blockchain Nodes

<details>
<summary>Example Thought Mainnet Config</summary>

```sh
# Make sure port & rpcport matches the
# thoughtcore.config.json ports for THT mainnet

# if using Thought Core v0.17+ prefix
# [main]

#port=20008
#rpcport=20009
rpcallowip=127.0.0.1

rpcuser=username
rpcpassword=password
```

</details>

### 3. Run Your Blockchain Nodes

<details>
<summary>Example Starting a Thought Node</summary>

```sh
# Path to your thought application and path to the config above
/Applications/Thought-Qt.app/Contents/MacOS/Thought-Qt -datadir=/Users/username/blockchains/thought-core/networks/mainnet/
```

</details>

### 4. Start Thoughtcore

```sh
npm run node
```

Thoughtcore will begin using your blockchain nodes to synchronize its own database so that you can use standardized queries to get data from each of your supported blockchains.

## API Documentation

- [REST API parameters and example responses](./docs/api-documentation.md)

- [Websockets API namespaces, event names and parameters](./docs/sockets-api.md)

- [Testing Thoughtcore-node in RegTest](./docs/wallet-guide.md)

## Contributing

See [CONTRIBUTING.md](../../Contributing.md) on the main thoughtcore repo for information about how to contribute.

## License

Code released under [the MIT license](https://github.com/thoughtnetwork/thoughtcore/blob/master/LICENSE).

Copyright 2013-2023 Thought, Inc. Thoughtcore is a trademark maintained by Thought, Inc.
