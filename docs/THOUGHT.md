
## Package Information

./packages contains the sources of all of the following:

APP - Insight - Blockchain Explorer utilizing mongodb, api from thoughtcore-node
APP - thoughtcore-node - A Webapi for retrieving blockchain information through rpc and fetch from db
APP - thoughtcore-wallet-service - A Webapi for Multisig HD Wallet creation and operation

TOOL - thoughtcore-build - A tool to add tasks to gulp
TOOL - thoughtcore-wallet - A Command Line tool for using TWS

LIB - thoughtcore-wallet-client - Client Lib for using TWS features
LIB - thoughtcore-client - Client Lib for using thoughtcore-node features
LIB - thoughtcore-lib - Lib for general javascript use with Thought network
LIB - thoughtcore-mnemonic - Lib for Thought Mnemonics
LIB - thoughtcore-p2p - Lib for Thought P2P networking features 
LIB - crypto-wallet-core - A coin-agnostic wallet library for creating transactions, signing, and address derivation.
LIB - crypto-rpc - Lib for utilizing rpc features

### Setup Information

Production setup can be performed through the use of docker, and docker compose up on the production machine

### Networking Information
Every app exists on one machine, with 2 or more subdomains pointing to different apps

#### Insight networking

If two instances of Insight, but seprate main and test subdomains

Insight Mainnet - exp2.thought.live - Port 443 -> Virtual Server in apache2 -> Port 3000
Insight Testnet - ext2.thought.live - Port 443 -> Virtual Server in apache2 -> Port 3002

If one instance of Insight, but seprate main and test subdomains (Somehow the subdomain needs to be parsed internal)
exp2.thought.live (mainnet) or ext2.thought.live (testnet) - Port 443 -> Virtual Server in apache2 -> Port 3000 -> Application can determine exp2 is mainnet and ext2 is testnet

If one instance of Insight, but just one subdomain for insight
insight.thought.live - Port 443 -> Virtual Server in apache2 -> Port 3000 -> User select in application between mainnet and testnet

#### Thoughtcore-node API networking
tapi.thought.live - Port 443 -> Virtual Server in apache2 -> Port 3004 -> (Config needs to be set in thoughtcore-node for express)

#### Mongo networking
Mongo is not exposed, should be local at 'mongodb://0.0.0.0:27017/tws' (with tws being the dbname if using tws db)




## TWS Configuration (Not required for just insight)

Configuration for all required modules can be specified in https://github.com/thoughtnetwork/thoughtcore-wallet-service/blob/master/config.js

TWS is composed of 4 separate node services -
Message Broker - messagebroker/messagebroker.js
Blockchain Monitor - bcmonitor/bcmonitor.js (This service talks to the Blockchain Explorer service configured under blockchainExplorerOpts - see Configure blockchain service below.)
Email Service - emailservice/emailservice.js
Thoughtcore Wallet Service - tws.js


### Configure MongoDB

Example configuration for connecting to the MongoDB instance:

```javascript
  storageOpts: {
    mongoDb: {
      uri: 'mongodb://localhost:27017/tws',
    },
  }
```

### Configure Message Broker service

Example configuration for connecting to message broker service:

```javascript
  messageBrokerOpts: {
    messageBrokerServer: {
      url: 'http://localhost:3380',
    },
  }
```

### Configure blockchain service. Thoughtcore v8 is required.

Note: this service will be used by blockchain monitor service as well as by TWS itself.
An example of this configuration is:

```javascript
  blockchainExplorerOpts: {
      'tht': {
        livenet: {
            provider: 'v8',
            url: 'https://exp2.thought.live:443',
         },
        testnet: {
            provider: 'v8',
            url: 'https://ext2.thought.live:443',
         },
      },
  }
```

## Setup Guide For Thought-Core Node (Which operates with TWS)

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