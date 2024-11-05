import { Deriver } from 'crypto-wallet-core';
import _ from 'lodash';
import { ChainService } from '../chain/index.ts';
import { Constants } from '../common/constants.ts';
import { Defaults } from '../common/defaults.ts';
import { Utils } from '../common/utils.ts';
import { AddressManager } from './addressmanager.ts';

const $ = require('preconditions').singleton();


export interface IAddress {
  version: string;
  createdOn: number;
  address: string;
  walletId: string;
  isChange: boolean;
  isEscrow: boolean;
  path: string;
  publicKeys: string[];
  coin: string;
  chain: string;
  network: string;
  type: string;
  hasActivity: boolean;
  beRegistered: boolean;
}

export class Address {
  version: string;
  createdOn: number;
  address: string;
  walletId: string;
  isChange: boolean;
  isEscrow: boolean;
  path: string;
  publicKeys: string[];
  coin: string;
  chain: string;
  network: string;
  type: string;
  hasActivity: boolean;
  beRegistered: boolean;

  static Thoughtcore = {
    tht: require('thoughtcore-lib'),
    bch: require('thoughtcore-lib-cash'),
    doge: require('thoughtcore-lib-doge'),
    ltc: require('thoughtcore-lib-ltc')
  };

  static create(opts) {
    opts = opts || {};

    const x = new Address();

    opts.chain = opts.chain || ChainService.getChain(opts.coin); // getChain -> backwards compatibility
    $.checkArgument(Utils.checkValueInCollection(opts.chain, Constants.CHAINS));

    x.version = '1.0.0';
    x.createdOn = Math.floor(Date.now() / 1000);
    x.address = opts.address;
    x.walletId = opts.walletId;
    x.isChange = opts.isChange;
    x.isEscrow = opts.isEscrow;
    x.path = opts.path;
    x.publicKeys = opts.publicKeys;
    x.coin = opts.chain;
    x.chain = opts.chain;
    x.network = Address.Thoughtcore[opts.chain]
      ? Address.Thoughtcore[opts.chain].Address(x.address).toObject().network
      : opts.network;
    x.type = opts.type || Constants.SCRIPT_TYPES.P2SH;
    x.hasActivity = undefined;
    x.beRegistered = null;
    return x;
  }

  static fromObj(obj) {
    const x = new Address();

    x.version = obj.version;
    x.createdOn = obj.createdOn;
    x.address = obj.address;
    x.walletId = obj.walletId;
    x.coin = obj.chain || ChainService.getChain(obj.coin);
    x.chain = x.coin;
    x.network = Utils.getNetworkName(x.chain, obj.network) || obj.network;
    x.isChange = obj.isChange;
    x.isEscrow = obj.isEscrow;
    x.path = obj.path;
    x.publicKeys = obj.publicKeys;
    x.type = obj.type || Constants.SCRIPT_TYPES.P2SH;
    x.hasActivity = obj.hasActivity;
    x.beRegistered = obj.beRegistered;
    return x;
  }

  static _deriveAddress(scriptType, publicKeyRing, path, m, chain, network, noNativeCashAddr, escrowInputs?, hardwareSourcePublicKey?) {
    $.checkArgument(Utils.checkValueInCollection(scriptType, Constants.SCRIPT_TYPES));

    if (hardwareSourcePublicKey) {
      const thoughtcoreAddress = Deriver.getAddress(chain.toUpperCase(), network, hardwareSourcePublicKey, scriptType);
      return {
        address: thoughtcoreAddress.toString(),
        path,
        publicKeys: [hardwareSourcePublicKey]
      }
    }

    let publicKeys = _.map(publicKeyRing, item => {
      const xpub = Address.Thoughtcore[chain]
        ? new Address.Thoughtcore[chain].HDPublicKey(item.xPubKey)
        : new Address.Thoughtcore.tht.HDPublicKey(item.xPubKey);
      return xpub.deriveChild(path).publicKey;
    });

    let thoughtcoreAddress;
    switch (scriptType) {
      case Constants.SCRIPT_TYPES.P2WSH:
        const nestedWitness = false;
        thoughtcoreAddress = Address.Thoughtcore[chain].Address.createMultisig(
          publicKeys,
          m,
          network,
          nestedWitness,
          'witnessscripthash'
        );
        break;
      case Constants.SCRIPT_TYPES.P2SH:
        if (escrowInputs) {
          var xpub = new Address.Thoughtcore[chain].HDPublicKey(publicKeyRing[0].xPubKey);
          const inputPublicKeys = escrowInputs.map(input => xpub.deriveChild(input.path).publicKey);
          thoughtcoreAddress = Address.Thoughtcore[chain].Address.createEscrow(inputPublicKeys, publicKeys[0], network);
          publicKeys = [publicKeys[0], ...inputPublicKeys];
        } else {
          thoughtcoreAddress = Address.Thoughtcore[chain].Address.createMultisig(publicKeys, m, network);
        }
        break;
      case Constants.SCRIPT_TYPES.P2WPKH:
        thoughtcoreAddress = Address.Thoughtcore[chain].Address.fromPublicKey(publicKeys[0], network, 'witnesspubkeyhash');
        break;
      case Constants.SCRIPT_TYPES.P2PKH:
        $.checkState(
          _.isArray(publicKeys) && publicKeys.length == 1,
          'Failed state: publicKeys length < 1 or publicKeys not an array at <_deriveAddress()>'
        );

        if (Address.Thoughtcore[chain]) {
          thoughtcoreAddress = Address.Thoughtcore[chain].Address.fromPublicKey(publicKeys[0], network);
        } else {
          const { addressIndex, isChange } = new AddressManager().parseDerivationPath(path);
          const [{ xPubKey }] = publicKeyRing;
          thoughtcoreAddress = Deriver.deriveAddress(chain.toUpperCase(), network, xPubKey, addressIndex, isChange);
        }
        break;
    }

    let addrStr = thoughtcoreAddress.toString(true);
    if (noNativeCashAddr && chain == 'bch') {
      addrStr = thoughtcoreAddress.toLegacyAddress();
    }

    return {
      // bws still use legacy addresses for BCH
      address: addrStr,
      path,
      publicKeys: _.invokeMap(publicKeys, 'toString')
    };
  }

  // noNativeCashAddr only for testing
  static derive(
    walletId,
    scriptType,
    publicKeyRing,
    path,
    m,
    coin,
    network,
    isChange,
    chain,
    noNativeCashAddr = false,
    escrowInputs?,
    hardwareSourcePublicKey?
  ) {
    const raw = Address._deriveAddress(
      scriptType,
      publicKeyRing,
      path,
      m,
      chain || ChainService.getChain(coin), // getChain -> backwards compatibility
      network,
      noNativeCashAddr,
      escrowInputs,
      hardwareSourcePublicKey,
    );
    return Address.create(
      _.extend(raw, {
        coin,
        chain,
        network,
        walletId,
        type: scriptType,
        isChange,
        isEscrow: !!escrowInputs
      })
    );
  }
}
