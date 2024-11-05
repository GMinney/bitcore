const testConfig = require('./thoughtcore-test.config.json');

testConfig.thoughtcoreNode.chains.THT.regtest.trustedPeers[0].host = 'thought';
testConfig.thoughtcoreNode.chains.THT.regtest.rpc.host = 'thought';

testConfig.thoughtcoreNode.chains.ETH.regtest.trustedPeers[0].host = 'geth';
testConfig.thoughtcoreNode.chains.ETH.regtest.trustedPeers[0].port = 30303;
testConfig.thoughtcoreNode.chains.ETH.regtest.providers[0].host = 'erigon';
testConfig.thoughtcoreNode.chains.ETH.regtest.providers[0].port = 8545;
testConfig.thoughtcoreNode.chains.ETH.regtest.providers[1].host = 'geth';
testConfig.thoughtcoreNode.chains.ETH.regtest.providers[1].port = 8546;

testConfig.thoughtcoreNode.chains.MATIC.regtest.trustedPeers[0].host = 'geth';
testConfig.thoughtcoreNode.chains.MATIC.regtest.trustedPeers[0].port = 30303;
testConfig.thoughtcoreNode.chains.MATIC.regtest.providers[0].host = 'geth';
testConfig.thoughtcoreNode.chains.MATIC.regtest.providers[0].port = 8546;

testConfig.thoughtcoreNode.chains.XRP.testnet.provider.host = 'rippled';
testConfig.thoughtcoreNode.chains.XRP.testnet.provider.port = 6006;
testConfig.thoughtcoreNode.chains.XRP.testnet.provider.dataHost = 'rippled';

module.exports = testConfig;
