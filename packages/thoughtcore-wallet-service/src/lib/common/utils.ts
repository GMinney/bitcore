import * as CWC from 'crypto-wallet-core';
import _ from 'lodash';
import Config from '../../config.ts';
import { logger } from '../logger.ts';
import { Defaults } from "./defaults.ts";
import { Constants } from './constants.ts';

const $ = require('preconditions').singleton();
const thoughtcore = require('thoughtcore-lib');
const crypto = thoughtcore.crypto;
const secp256k1 = require('secp256k1');
const Thoughtcore = require('thoughtcore-lib');
const Thoughtcore_ = {
  tht: Thoughtcore,
  bch: require('thoughtcore-lib-cash'),
  doge: require('thoughtcore-lib-doge'),
  ltc: require('thoughtcore-lib-ltc')
};

export class Utils {
  static getMissingFields(obj, args) {
    args = [].concat(args);
    if (!_.isObject(obj)) return args;
    const missing = _.filter(args, arg => {
      return !obj.hasOwnProperty(arg);
    });
    return missing;
  }

  /**
   *
   * @desc rounds a JAvascript number
   * @param number
   * @return {number}
   */
  static strip(number) {
    return parseFloat(number.toPrecision(12));
  }

  /* TODO: It would be nice to be compatible with thoughtd signmessage. How
   * the hash is calculated there? */
  static hashMessage(text, noReverse) {
    $.checkArgument(text);
    const buf = Buffer.from(text);
    let ret = crypto.Hash.sha256sha256(buf);
    if (!noReverse) {
      ret = new thoughtcore.encoding.BufferReader(ret).readReverse();
    }
    return ret;
  }

  static verifyMessage(message, signature, publicKey) {
    $.checkArgument(message);

    const flattenedMessage = _.isArray(message) ? _.join(message) : message;
    const hash = Utils.hashMessage(flattenedMessage, true);

    const sig = this._tryImportSignature(signature);
    if (!sig) {
      return false;
    }

    const publicKeyBuffer = this._tryImportPublicKey(publicKey);
    if (!publicKeyBuffer) {
      return false;
    }

    return this._tryVerifyMessage(hash, sig, publicKeyBuffer);
  }

  static _tryImportPublicKey(publicKey) {
    let publicKeyBuffer = publicKey;
    try {
      if (!Buffer.isBuffer(publicKey)) {
        publicKeyBuffer = Buffer.from(publicKey, 'hex');
      }
      return publicKeyBuffer;
    } catch (e) {
      logger.error('_tryImportPublicKey encountered an error: %o', e);
      return false;
    }
  }

  static _tryImportSignature(signature) {
    try {
      let signatureBuffer = signature;
      if (!Buffer.isBuffer(signature)) {
        signatureBuffer = Buffer.from(signature, 'hex');
      }
      // uses the native module (c++) for performance vs thoughtcore lib (javascript)
      return secp256k1.signatureImport(signatureBuffer);
    } catch (e) {
      logger.error('_tryImportSignature encountered an error: %o', e);
      return false;
    }
  }

  static _tryVerifyMessage(hash, sig, publicKeyBuffer) {
    try {
      // uses the native module (c++) for performance vs thoughtcore lib (javascript)
      return secp256k1.ecdsaVerify(sig, hash, publicKeyBuffer);
    } catch (e) {
      logger.error('_tryVerifyMessage encountered an error: %o', e);
      return false;
    }
  }

  static formatAmount(notions, unit, opts) {
    const UNITS = Object.entries(CWC.Constants.UNITS).reduce((units, [currency, currencyConfig]) => {
      units[currency] = {
        toNotions: currencyConfig.toNotions,
        maxDecimals: currencyConfig.short.maxDecimals,
        minDecimals: currencyConfig.short.minDecimals
      };
      return units;
    }, {} as { [currency: string]: { toNotions: number; maxDecimals: number; minDecimals: number } });

    $.shouldBeNumber(notions);

    function addSeparators(nStr, thousands, decimal, minDecimals) {
      nStr = nStr.replace('.', decimal);
      const x = nStr.split(decimal);
      let x0 = x[0];
      let x1 = x[1];

      x1 = _.dropRightWhile(x1, (n, i) => {
        return n == '0' && i >= minDecimals;
      }).join('');
      const x2 = x.length > 1 ? decimal + x1 : '';

      x0 = x0.replace(/\B(?=(\d{3})+(?!\d))/g, thousands);
      return x0 + x2;
    }

    opts = opts || {};

    if (!UNITS[unit] && !opts.decimals && !opts.toNotions) {
      return Number(notions).toLocaleString();
    }

    const u = _.assign(UNITS[unit], opts);
    var decimals = opts.decimals ? opts.decimals : u;
    var toNotions = opts.toNotions ? opts.toNotions : u.toNotions;

    const amount = (notions / toNotions).toFixed(decimals.maxDecimals);
    return addSeparators(amount, opts.thousandsSeparator || ',', opts.decimalSeparator || '.', decimals.minDecimals);
  }

  static formatAmountInTht(amount) {
    return (
      Utils.formatAmount(amount, 'tht', {
        minDecimals: 8,
        maxDecimals: 8
      }) + 'tht'
    );
  }

  static formatUtxos(utxos) {
    if (_.isEmpty(utxos)) return 'none';
    return _.map([].concat(utxos), i => {
      const amount = Utils.formatAmountInTht(i.notions);
      const confirmations = i.confirmations ? i.confirmations + 'c' : 'u';
      return amount + '/' + confirmations;
    }).join(', ');
  }

  static formatRatio(ratio) {
    return (ratio * 100).toFixed(4) + '%';
  }

  static formatSize(size) {
    return (size / 1000).toFixed(4) + 'kB';
  }

  static parseVersion(version) {
    const v: {
      agent?: string;
      major?: number;
      minor?: number;
      patch?: number;
    } = {};

    if (!version) return null;

    let x = version.split('-');
    if (x.length != 2) {
      v.agent = version;
      return v;
    }
    v.agent = _.includes(['bwc', 'bws'], x[0]) ? 'bwc' : x[0];
    x = x[1].split('.');
    v.major = x[0] ? parseInt(x[0]) : null;
    v.minor = x[1] ? parseInt(x[1]) : null;
    v.patch = x[2] ? parseInt(x[2]) : null;

    return v;
  }

  static parseAppVersion(agent) {
    const v: {
      app?: string;
      major?: number;
      minor?: number;
      patch?: number;
    } = {};
    if (!agent) return null;
    agent = agent.toLowerCase();

    let w;
    w = agent.indexOf('copay');
    if (w >= 0) {
      v.app = 'copay';
    } else {
      w = agent.indexOf('thoughtnetwork');
      if (w >= 0) {
        v.app = 'thoughtnetwork';
      } else {
        v.app = 'other';
        return v;
      }
    }

    const version = agent.substr(w + v.app.length);
    const x = version.split('.');
    v.major = x[0] ? parseInt(x[0].replace(/\D/g, '')) : null;
    v.minor = x[1] ? parseInt(x[1]) : null;
    v.patch = x[2] ? parseInt(x[2]) : null;

    return v;
  }

  static getIpFromReq(req): string {
    if (req.headers) {
      if (req.headers['x-forwarded-for']) return req.headers['x-forwarded-for'].split(',')[0];
      if (req.headers['x-real-ip']) return req.headers['x-real-ip'].split(',')[0];
    }
    if (req.ip) return req.ip;
    if (req.connection && req.connection.remoteAddress) return req.connection.remoteAddress;
    return '';
  }

  static checkValueInCollection(value, collection) {
    if (!value || !_.isString(value)) return false;
    return _.includes(_.values(collection), value);
  }

  static getAddressCoin(address) {
    try {
      new Thoughtcore_['tht'].Address(address);
      return 'tht';
    } catch (e) {
      try {
        new Thoughtcore_['bch'].Address(address);
        return 'bch';
      } catch (e) {
        try {
          new Thoughtcore_['doge'].Address(address);
          return 'doge';
        } catch (e) {
          try {
            new Thoughtcore_['ltc'].Address(address);
            return 'ltc';
          } catch (e) {
            return;
          }
        }
      }
    }
  }

  static translateAddress(address, coin) {
    const origCoin = Utils.getAddressCoin(address);
    const origAddress = new Thoughtcore_[origCoin].Address(address);
    const origObj = origAddress.toObject();

    const result = Thoughtcore_[coin].Address.fromObject(origObj);
    return coin == 'bch' ? result.toLegacyAddress() : result.toString();
  }

  static compareNetworks(network1, network2, chain) {
    network1 = network1 ? this.getNetworkName(chain, network1.toLowerCase()) : null;
    network2 = network2 ? this.getNetworkName(chain, network2.toLowerCase()) : null;

    if (network1 == network2) return true;
    if (Config.allowRegtest && ['testnet', 'regtest'].includes(this.getNetworkType(network1)) && ['testnet', 'regtest'].includes(this.getNetworkType(network2))) return true;
    return false;
  }

  // Good for going from generic 'testnet' to specific 'testnet3', 'sepolia', etc
  static getNetworkName(chain, network) {
    const aliases = Constants.NETWORK_ALIASES[chain];
    if (aliases && aliases[network]) {
      return aliases[network];
    }
    return network;
  }

  // Good for going from specific 'testnet3', 'sepolia', etc to generic 'testnet'
  static getGenericName(network) {
    if (network === 'mainnet') return 'livenet';
    const isTestnet = !!Object.keys(Constants.NETWORK_ALIASES).find(key => Constants.NETWORK_ALIASES[key].testnet === network);
    if (isTestnet) return 'testnet';
    return network;
  }

  static getNetworkType(network) {
    if (['mainnet', 'livenet'].includes(network)) {
      return 'mainnet';
    }
    if (network === 'regtest') {
       return 'regtest';
    }
    return 'testnet';
  }
}
