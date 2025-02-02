import _ from "lodash";
import Address from "./address.js";
import Base58Check from "./encoding/base58check.js";
import BN from "./crypto/bn.js";
import JSUtil from "./util/js.js";
import Networks from "./networks.js";
import Point from "./crypto/point.js";
import PublicKey from "./publickey.js";
import Random from "./crypto/random.js";
import $ from "./util/preconditions.js";
import TaggedHash from "./crypto/taggedhash.js";

'use strict';

/**
 * Instantiate a PrivateKey from a BN, Buffer and WIF.
 *
 * @example
 * ```javascript
 * // generate a new random key
 * var key = PrivateKey();
 *
 * // get the associated address
 * var address = key.toAddress();
 *
 * // encode into wallet export format
 * var exported = key.toWIF();
 *
 * // instantiate from the exported (and saved) private key
 * var imported = fromWIF(exported);
 * ```
 *
 * @param {string} data - The encoded data in various formats
 * @param {Network|string=} network - a {@link Network} object, or a string with the network name
 * @returns {PrivateKey} A new valid instance of an PrivateKey
 * @constructor
 */
export class PrivateKey {
  constructor(data, network) {
    if (!(this instanceof PrivateKey)) {
      return new PrivateKey(data, network);
    }
    if (data instanceof PrivateKey) {
      return data;
    }

    var info = this._classifyArguments(data, network);

    // validation
    if (!info.bn || info.bn.cmp(new BN(0)) === 0) {
      throw new TypeError('Number can not be equal to zero, undefined, null or false');
    }
    if (!info.bn.lt(Point.getN())) {
      throw new TypeError('Number must be less than N');
    }
    if (typeof (info.network) === 'undefined') {
      throw new TypeError('Must specify the network ("livenet" or "testnet")');
    }

    JSUtil.defineImmutable(this, {
      bn: info.bn,
      compressed: info.compressed,
      network: info.network
    });

    Object.defineProperty(this, 'publicKey', {
      configurable: false,
      enumerable: true,
      get: this.toPublicKey.bind(this)
    });

    return this;
  }

  /**
   * Constructors
   */

  /**
   * Internal function to get a random Big Number (BN)
   *
   * @returns {BN} A new randomly generated BN
   * @private
   */
  _getRandomBN = function () {
    var condition;
    var bn;
    do {
      var privbuf = Random.getRandomBuffer(32);
      bn = BN.fromBuffer(privbuf);
      condition = bn.lt(Point.getN());
    } while (!condition);
    return bn;
  };

  /**
   * Internal function to transform a WIF Buffer into a private key
   *
   * @param {Buffer} buf - An WIF string
   * @param {Network|string=} network - a {@link Network} object, or a string with the network name
   * @returns {Object} An object with keys: bn, network and compressed
   * @private
   */
  _transformBuffer = function (buf, network) {

    var info = {};

    if (buf.length === 32) {
      return this._transformBNBuffer(buf, network);
    }

    info.network = Networks.get(buf[0], 'privatekey');

    if (!info.network) {
      throw new Error('Invalid network');
    }

    if (network && info.network !== Networks.get(network)) {
      throw new TypeError('Private key network mismatch');
    }

    if (buf.length === 1 + 32 + 1 && buf[1 + 32 + 1 - 1] === 1) {
      info.compressed = true;
    } else if (buf.length === 1 + 32) {
      info.compressed = false;
    } else {
      throw new Error('Length of buffer must be 33 (uncompressed) or 34 (compressed)');
    }

    info.bn = BN.fromBuffer(buf.subarray(1, 32 + 1));

    return info;
  };

  /**
   * Internal function to transform a BN buffer into a private key
   *
   * @param {Buffer} buf
   * @param {Network|string=} network - a {@link Network} object, or a string with the network name
   * @returns {object} an Object with keys: bn, network, and compressed
   * @private
   */
  _transformBNBuffer = function (buf, network) {
    var info = {};
    info.network = Networks.get(network) || Networks.defaultNetwork;
    info.bn = BN.fromBuffer(buf);
    info.compressed = false;
    return info;
  };

  /**
   * Internal function to transform a WIF string into a private key
   *
   * @param {string} buf - An WIF string
   * @returns {Object} An object with keys: bn, network and compressed
   * @private
   */
  _transformWIF = function (str, network) {
    return this._transformBuffer(Base58Check.decode(str), network);
  };

  /**
   * Instantiate a PrivateKey from a Buffer with the DER or WIF representation
   *
   * @param {Buffer} arg
   * @param {Network} network
   * @return {PrivateKey}
   */
  fromBuffer = function (arg, network) {
    return new PrivateKey(arg, network);
  };

  /**
   * Internal function to transform a JSON string on plain object into a private key
   * return this.
   *
   * @param {string} json - A JSON string or plain object
   * @returns {Object} An object with keys: bn, network and compressed
   * @private
   */
  _transformObject = function (json) {
    var bn = new BN(json.bn, 'hex');
    var network = Networks.get(json.network);
    return {
      bn: bn,
      network: network,
      compressed: json.compressed
    };
  };

  /**
   * Instantiate a PrivateKey from a WIF string
   *
   * @param {string} str - The WIF encoded private key string
   * @returns {PrivateKey} A new valid instance of PrivateKey
   */
  fromString = fromWIF = function (str) {
    $.checkArgument(_.isString(str), 'First argument is expected to be a string.');
    return new PrivateKey(str);
  };

  /**
   * Instantiate a PrivateKey from a plain JavaScript object
   *
   * @param {Object} obj - The output from privateKey.toObject()
   */
  fromObject = function (obj) {
    $.checkArgument(_.isObject(obj), 'First argument is expected to be an object.');
    return new PrivateKey(obj);
  };

  /**
   * Instantiate a PrivateKey from random bytes
   *
   * @param {string=} network - Either "livenet" or "testnet"
   * @returns {PrivateKey} A new valid instance of PrivateKey
   */
  fromRandom = function (network) {
    var bn = this._getRandomBN();
    return new PrivateKey(bn, network);
  };

  /**
   * Check if there would be any errors when initializing a PrivateKey
   *
   * @param {string} data - The encoded data in various formats
   * @param {string=} network - Either "livenet" or "testnet"
   * @returns {null|Error} An error if exists
   */
  getValidationError = function (data, network) {
    var error;
    try {
      /* jshint nonew: false */
      new PrivateKey(data, network);
    } catch (e) {
      error = e;
    }
    return error;
  };

  /**
   * Check if the parameters are valid
   *
   * @param {string} data - The encoded data in various formats
   * @param {string=} network - Either "livenet" or "testnet"
   * @returns {Boolean} If the private key is would be valid
   */
  isValid = function (data, network) {
    if (!data) {
      return false;
    }
    return !this.getValidationError(data, network);
  };


  /**
   * Methods
   */

  /**
 * Internal helper to instantiate PrivateKey internal `info` object from
 * different kinds of arguments passed to the constructor.
 *
 * @param {*} data
 * @param {Network|string=} network - a {@link Network} object, or a string with the network name
 * @return {Object}
 */
  _classifyArguments = function (data, network) {
    /* jshint maxcomplexity: 10 */
    var info = {
      compressed: true,
      network: network ? Networks.get(network) : Networks.defaultNetwork
    };

    // detect type of data
    if (_.isUndefined(data) || _.isNull(data)) {
      info.bn = this._getRandomBN();
    } else if (data instanceof BN) {
      info.bn = data;
    } else if (data instanceof Buffer || data instanceof Uint8Array) {
      info = this._transformBuffer(data, network);
    } else if (data.bn && data.network) {
      info = this._transformObject(data);
    } else if (!network && Networks.get(data)) {
      info.bn = this._getRandomBN();
      info.network = Networks.get(data);
    } else if (typeof (data) === 'string') {
      if (JSUtil.isHexa(data)) {
        info.bn = new BN(Buffer.from(data, 'hex'));
      } else {
        info = this._transformWIF(data, network);
      }
    } else {
      throw new TypeError('First argument is an unrecognized data type.');
    }
    return info;
  };

  /**
   * Will output the PrivateKey encoded as hex string
   *
   * @returns {string}
   */
  toString = function () {
    return this.toBuffer().toString('hex');
  };

  /**
   * Will output the PrivateKey to a WIF string
   *
   * @returns {string} A WIP representation of the private key
   */
  toWIF = function () {
    var network = this.network;
    var compressed = this.compressed;

    var buf;
    if (compressed) {
      buf = Buffer.concat([Buffer.from([network.privatekey]),
      this.bn.toBuffer({ size: 32 }),
      Buffer.from([0x01])]);
    } else {
      buf = Buffer.concat([Buffer.from([network.privatekey]),
      this.bn.toBuffer({ size: 32 })]);
    }

    return Base58Check.encode(buf);
  };

  /**
   * Will return the private key as a BN instance
   *
   * @returns {BN} A BN instance of the private key
   */
  toBigNumber = function () {
    return this.bn;
  };

  /**
   * Will return the private key as a BN buffer
   *
   * @returns {Buffer} A buffer of the private key
   */
  toBuffer = function () {
    return this.bn.toBuffer({ size: 32 });
  };

  /**
   * WARNING: This method will not be officially supported until v1.0.0.
   *
   *
   * Will return the private key as a BN buffer without leading zero padding
   *
   * @returns {Buffer} A buffer of the private key
   */
  toBufferNoPadding = function () {
    return this.bn.toBuffer();
  };

  /**
   * Will return the corresponding public key
   *
   * @returns {PublicKey} A public key generated from the private key
   */
  toPublicKey = function () {
    if (!this._pubkey) {
      this._pubkey = PublicKey.fromPrivateKey(this);
    }
    return this._pubkey;
  };

  /**
   * Will return an address for the private key
   * @param {Network=} network - optional parameter specifying
   * @param {string} type - Either 'pubkeyhash', 'witnesspubkeyhash', or 'scripthash'
   * the desired network for the address
   *
   * @returns {Address} An address generated from the private key
   */
  toAddress = function (network, type) {
    var pubkey = this.toPublicKey();
    return Address.fromPublicKey(pubkey, network || this.network, type);
  };

  /**
   * @returns {Object} A plain object representation
   */
  toObject = toJSON = function toObject() {
    return {
      bn: this.bn.toString('hex'),
      compressed: this.compressed,
      network: this.network.toString()
    };
  };

  /**
   * Create a tweaked version of this private key
   * @param {Buffer} merkleRoot (optional)
   * @returns {{ tweakedPrivKey: Buffer }}
   */
  createTapTweak = function (merkleRoot) {
    const order = Point.getN();
    const P = Point.getG().mul(this.bn);
    const secKey = P.y.isEven() ? this.bn : order.sub(this.bn);
    const taggedWriter = new TaggedHash('TapTweak');
    taggedWriter.write(P.x.toBuffer({ size: 32 }));

    if (merkleRoot) {
      $.checkArgument(Buffer.isBuffer(merkleRoot) && merkleRoot.length === 32, 'merkleRoot must be 32 byte buffer');
      taggedWriter.write(merkleRoot);
    }
    const tweakHash = taggedWriter.finalize();

    $.checkState(BN.fromBuffer(tweakHash).lt(order), 'TapTweak hash failed secp256k1 order check');
    return {
      tweakedPrivKey: secKey.add(new BN(tweakHash)).mod(order).toBuffer({ size: 32 })
    };
  };

  /**
   * Will return a string formatted for the console
   *
   * @returns {string} Private key
   */
  inspect = function () {
    var uncompressed = !this.compressed ? ', uncompressed' : '';
    return '<PrivateKey: ' + this.toString() + ', network: ' + this.network + uncompressed + '>';
  };

};


export default PrivateKey;
