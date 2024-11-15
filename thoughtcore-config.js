const Config = require('./thoughtcore.config.json');

Config.thoughtcoreNode.chains.THT.regtest.trustedPeers[0].host = 'thought';
Config.thoughtcoreNode.chains.THT.regtest.rpc.host = 'thought';

Config.thoughtcoreNode.chains.ETH.regtest.trustedPeers[0].host = 'geth';
Config.thoughtcoreNode.chains.ETH.regtest.trustedPeers[0].port = 30303;
Config.thoughtcoreNode.chains.ETH.regtest.providers[0].host = 'erigon';
Config.thoughtcoreNode.chains.ETH.regtest.providers[0].port = 8545;
Config.thoughtcoreNode.chains.ETH.regtest.providers[1].host = 'geth';
Config.thoughtcoreNode.chains.ETH.regtest.providers[1].port = 8546;


module.exports = Config;
