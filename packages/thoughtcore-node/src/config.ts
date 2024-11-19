import * as _ from 'lodash';
import { cpus, homedir } from 'os';
import { ConfigType } from './types/Config';
import parseArgv from './utils/parseArgv';
let program = parseArgv([], ['config']);

function findConfig(): ConfigType | undefined {
  let foundConfig;
  const envConfigPath = process.env.THOUGHTCORE_CONFIG_PATH;
  const argConfigPath = program.config;
  const configFileName = 'thoughtcore.config.json';
  let thoughtcoreConfigPaths = [
    `${homedir()}/${configFileName}`,
    `../../../../${configFileName}`,
    `../../${configFileName}`
  ];
  const overrideConfig = argConfigPath || envConfigPath;
  if (overrideConfig) {
    thoughtcoreConfigPaths.unshift(overrideConfig);
  }
  // No config specified. Search home, thoughtcore and cur directory
  for (let path of thoughtcoreConfigPaths) {
    if (!foundConfig) {
      try {
        const expanded = path[0] === '~' ? path.replace('~', homedir()) : path;
        const thoughtcoreConfig = require(expanded) as { thoughtcoreNode: ConfigType };
        foundConfig = thoughtcoreConfig.thoughtcoreNode;
      } catch (e) {
        foundConfig = undefined;
      }
    }
  }
  return foundConfig;
}

function setTrustedPeers(config: ConfigType): ConfigType {
  for (let [chain, chainObj] of Object.entries(config)) {
    for (let network of Object.keys(chainObj)) {
      let env = process.env;
      const envString = `TRUSTED_${chain.toUpperCase()}_${network.toUpperCase()}_PEER`;
      if (env[envString]) {
        let peers = config.chains[chain][network].trustedPeers || [];
        peers.push({
          host: env[envString] as string,
          port: env[`${envString}_PORT`] as string
        });
        config.chains[chain][network].trustedPeers = peers;
      }
    }
  }
  return config;
}
const Config = function (): ConfigType {
  let config: ConfigType = {
    maxPoolSize: 50,
    port: 3000,
    dbUrl: process.env.DB_URL || '',
    dbHost: process.env.DB_HOST || '127.0.0.1',
    dbName: process.env.DB_NAME || 'thoughtcore',
    dbPort: process.env.DB_PORT || '27017',
    dbUser: process.env.DB_USER || '',
    dbPass: process.env.DB_PASS || '',
    numWorkers: cpus().length,
    chains: {},
    aliasMapping: {
      chains: {},
      networks: {}
    },
    modules: ['./thought'],
    services: {
      api: {
        rateLimiter: {
          disabled: false,
          whitelist: ['::ffff:127.0.0.1', '::1']
        },
        wallets: {
          allowCreationBeforeCompleteSync: false,
          allowUnauthenticatedCalls: false
        }
      },
      event: {
        onlyWalletEvents: false
      },
      p2p: {},
      socket: {
        bwsKeys: []
      },
      storage: {}
    },
    externalProviders: {
      moralis: {
        apiKey: 'string'
      }
    }
  };

  let foundConfig = findConfig();
  const mergeCopyArray = (objVal, srcVal) => (objVal instanceof Array ? srcVal : undefined);
  config = _.mergeWith(config, foundConfig, mergeCopyArray);
  if (!Object.keys(config.chains).length) {
    Object.assign(config.chains, {
      THT: {
        mainnet: {
          chainSource: 'p2p',
          trustedPeers: [{ host: 'phi.thought.live', port: 10618 }, { host: 'idea-01.insufficient-light.com', port: 10618 }, { host: 'intuition-01.insufficient-light.com', port: 10618 }],
          rpc: {
            host: '127.0.0.1',
            port: 10617,
            username: 'username',
            password: 'password'
          }
        }
      }
    });
  }
  config = setTrustedPeers(config);
  return config;
};

export default Config();
