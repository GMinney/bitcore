import _ from "lodash";
import $ from "./util/preconditions.js";
import BN from "./crypto/bn.js";
import Base58 from "./encoding/base58.js";
import Base58Check from "./encoding/base58check.js";
import Hash from "./crypto/hash.js";
import HDPrivateKey from "./hdprivatekey.js";
import Network from "./networks.js";
import Point from "./crypto/point.js";
import PublicKey from "./publickey.js";
import thoughtcoreErrors from "./errors/index.js";
import assert from "assert";
import JSUtil from "./util/js.js";
import BufferUtil from "./util/buffer.js";

'use strict';
var errors = thoughtcoreErrors;
var hdErrors = thoughtcoreErrors.HDPublicKey;

/**
 * The representation of an hierarchically derived public key.
 *
 * See https://github.com/thought/bips/blob/master/bip-0032.mediawiki
 *
 * @constructor
 * @param {Object|string|Buffer} arg
 */
export class HDPublicKey {

  static Hardened = 0x80000000;
  static RootElementAlias = ['m', 'M'];
  
  static VersionSize = 4;
  static DepthSize = 1;
  static ParentFingerPrintSize = 4;
  static ChildIndexSize = 4;
  static ChainCodeSize = 32;
  static PublicKeySize = 33;
  static CheckSumSize = 4;
  
  static DataSize = 78;
  static SerializedByteSize = 82;
  
  static VersionStart = 0;
  static VersionEnd = HDPublicKey.VersionStart + HDPublicKey.VersionSize;
  static DepthStart = HDPublicKey.VersionEnd;
  static DepthEnd = HDPublicKey.DepthStart + HDPublicKey.DepthSize;
  static ParentFingerPrintStart = HDPublicKey.DepthEnd;
  static ParentFingerPrintEnd = HDPublicKey.ParentFingerPrintStart + HDPublicKey.ParentFingerPrintSize;
  static ChildIndexStart = HDPublicKey.ParentFingerPrintEnd;
  static ChildIndexEnd = HDPublicKey.ChildIndexStart + HDPublicKey.ChildIndexSize;
  static ChainCodeStart = HDPublicKey.ChildIndexEnd;
  static ChainCodeEnd = HDPublicKey.ChainCodeStart + HDPublicKey.ChainCodeSize;
  static PublicKeyStart = HDPublicKey.ChainCodeEnd;
  static PublicKeyEnd = HDPublicKey.PublicKeyStart + HDPublicKey.PublicKeySize;
  static ChecksumStart = HDPublicKey.PublicKeyEnd;
  static ChecksumEnd = HDPublicKey.ChecksumStart + HDPublicKey.CheckSumSize;


  constructor(arg) {
    if (arg instanceof HDPublicKey) {
      return arg;
    }
    if (!(this instanceof HDPublicKey)) {
      return new HDPublicKey(arg);
    }
    if (arg) {
      if (_.isString(arg) || BufferUtil.isBuffer(arg)) {
        var error = this.getSerializedError(arg);
        if (!error) {
          return this._buildFromSerialized(arg);
        } else if (BufferUtil.isBuffer(arg) && !this.getSerializedError(arg.toString())) {
          return this._buildFromSerialized(arg.toString());
        } else {
          if (error instanceof hdErrors.ArgumentIsPrivateExtended) {
            return new HDPrivateKey(arg).hdPublicKey;
          }
          throw error;
        }
      } else {
        if (_.isObject(arg)) {
          if (arg instanceof HDPrivateKey) {
            return this._buildFromPrivate(arg);
          } else {
            return this._buildFromObject(arg);
          }
        } else {
          throw new hdErrors.UnrecognizedArgument(arg);
        }
      }
    } else {
      throw new hdErrors.MustSupplyArgument();
    }
  }



/**
 * Verifies that a given path is valid.
 *
 * @param {string|number} arg
 * @return {boolean}
 */
isValidPath = function (arg) {
  if (_.isString(arg)) {
    var indexes = HDPrivateKey._getDerivationIndexes(arg);
    return indexes !== null && _.every(indexes, this.isValidPath);
  }

  if (_.isNumber(arg)) {
    return arg >= 0 && arg < HDPublicKey.Hardened;
  }

  return false;
};

/**
 * WARNING: This method is deprecated. Use deriveChild instead.
 *
 *
 * Get a derivated child based on a string or number.
 *
 * If the first argument is a string, it's parsed as the full path of
 * derivation. Valid values for this argument include "m" (which returns the
 * same public key), "m/0/1/40/2/1000".
 *
 * Note that hardened keys can't be derived from a public extended key.
 *
 * If the first argument is a number, the child with that index will be
 * derived. See the example usage for clarification.
 *
 * @example
 * ```javascript
 * var parent = new HDPublicKey('xpub...');
 * var child_0_1_2 = parent.derive(0).derive(1).derive(2);
 * var copy_of_child_0_1_2 = parent.derive("m/0/1/2");
 * assert(child_0_1_2.xprivkey === copy_of_child_0_1_2);
 * ```
 *
 * @param {string|number} arg
 */
derive = function (arg, hardened) {
  return this.deriveChild(arg, hardened);
};

/**
 * WARNING: This method will not be officially supported until v1.0.0.
 *
 *
 * Get a derivated child based on a string or number.
 *
 * If the first argument is a string, it's parsed as the full path of
 * derivation. Valid values for this argument include "m" (which returns the
 * same public key), "m/0/1/40/2/1000".
 *
 * Note that hardened keys can't be derived from a public extended key.
 *
 * If the first argument is a number, the child with that index will be
 * derived. See the example usage for clarification.
 *
 * @example
 * ```javascript
 * var parent = new HDPublicKey('xpub...');
 * var child_0_1_2 = parent.deriveChild(0).deriveChild(1).deriveChild(2);
 * var copy_of_child_0_1_2 = parent.deriveChild("m/0/1/2");
 * assert(child_0_1_2.xprivkey === copy_of_child_0_1_2);
 * ```
 *
 * @param {string|number} arg
 */
deriveChild = function (arg, hardened) {
  if (_.isNumber(arg)) {
    return this._deriveWithNumber(arg, hardened);
  } else if (_.isString(arg)) {
    return this._deriveFromString(arg);
  } else {
    throw new hdErrors.InvalidDerivationArgument(arg);
  }
};

_deriveWithNumber = function (index, hardened) {
  if (index >= HDPublicKey.Hardened || hardened) {
    throw new hdErrors.InvalidIndexCantDeriveHardened();
  }
  if (index < 0) {
    throw new hdErrors.InvalidPath(index);
  }

  var indexBuffer = BufferUtil.integerAsBuffer(index);
  var data = BufferUtil.concat([this.publicKey.toBuffer(), indexBuffer]);
  var hash = Hash.sha512hmac(data, this._buffers.chainCode);
  var leftPart = BN.fromBuffer(hash.slice(0, 32), { size: 32 });
  var chainCode = hash.slice(32, 64);

  var publicKey;
  try {
    publicKey = PublicKey.fromPoint(Point.getG().mul(leftPart).add(this.publicKey.point));
  } catch (e) {
    return this._deriveWithNumber(index + 1);
  }

  var derived = new HDPublicKey({
    network: this.network,
    depth: this.depth + 1,
    parentFingerPrint: this.fingerPrint,
    childIndex: index,
    chainCode: chainCode,
    publicKey: publicKey
  });

  return derived;
};

_deriveFromString = function (path) {
  /* jshint maxcomplexity: 8 */
  if (_.includes(path, "'")) {
    throw new hdErrors.InvalidIndexCantDeriveHardened();
  } else if (!this.isValidPath(path)) {
    throw new hdErrors.InvalidPath(path);
  }

  var indexes = HDPrivateKey._getDerivationIndexes(path);
  var derived = indexes.reduce(function (prev, index) {
    return prev._deriveWithNumber(index);
  }, this);

  return derived;
};

/**
 * Verifies that a given serialized public key in base58 with checksum format
 * is valid.
 *
 * @param {string|Buffer} data - the serialized public key
 * @param {string|Network=} network - optional, if present, checks that the
 *     network provided matches the network serialized.
 * @return {boolean}
 */
isValidSerialized = function (data, network) {
  return _.isNull(this.getSerializedError(data, network));
};

/**
 * Checks what's the error that causes the validation of a serialized public key
 * in base58 with checksum to fail.
 *
 * @param {string|Buffer} data - the serialized public key
 * @param {string|Network=} network - optional, if present, checks that the
 *     network provided matches the network serialized.
 * @return {errors|null}
 */
getSerializedError = function (data, network?) {
  /* jshint maxcomplexity: 10 */
  /* jshint maxstatements: 20 */
  if (!(_.isString(data) || BufferUtil.isBuffer(data))) {
    return new hdErrors.UnrecognizedArgument('expected buffer or string');
  }
  if (!Base58.validCharacters(data)) {
    return new errors.InvalidB58Char('(unknown)', data);
  }
  try {
    data = Base58Check.decode(data);
  } catch (e) {
    return new errors.InvalidB58Checksum(data);
  }
  if (data.length !== HDPublicKey.DataSize) {
    return new hdErrors.InvalidLength(data);
  }
  if (!_.isUndefined(network)) {
    var error = this._validateNetwork(data, network);
    if (error) {
      return error;
    }
  }
  var version = BufferUtil.integerFromBuffer(data.slice(0, 4));
  if (version === Network.livenet.xprivkey || version === Network.testnet.xprivkey) {
    return new hdErrors.ArgumentIsPrivateExtended();
  }
  return null;
};

_validateNetwork = function (data, networkArg) {
  var network = Network.get(networkArg);
  if (!network) {
    return new errors.InvalidNetworkArgument(networkArg);
  }
  var version = data.slice(HDPublicKey.VersionStart, HDPublicKey.VersionEnd);
  if (BufferUtil.integerFromBuffer(version) !== network.xpubkey) {
    return new errors.InvalidNetwork(version);
  }
  return null;
};

_buildFromPrivate = function (arg) {
  var args = _.clone(arg._buffers);
  var point = Point.getG().mul(BN.fromBuffer(args.privateKey));
  args.publicKey = Point.pointToCompressed(point);
  args.version = BufferUtil.integerAsBuffer(Network.get(BufferUtil.integerFromBuffer(args.version)).xpubkey);
  args.privateKey = undefined;
  args.checksum = undefined;
  args.xprivkey = undefined;
  return this._buildFromBuffers(args);
};

_buildFromObject = function (arg) {
  /* jshint maxcomplexity: 10 */
  // TODO: Type validation
  var buffers = {
    version: arg.network ? BufferUtil.integerAsBuffer(Network.get(arg.network).xpubkey) : arg.version,
    depth: _.isNumber(arg.depth) ? BufferUtil.integerAsSingleByteBuffer(arg.depth) : arg.depth,
    parentFingerPrint: _.isNumber(arg.parentFingerPrint) ? BufferUtil.integerAsBuffer(arg.parentFingerPrint) : arg.parentFingerPrint,
    childIndex: _.isNumber(arg.childIndex) ? BufferUtil.integerAsBuffer(arg.childIndex) : arg.childIndex,
    chainCode: _.isString(arg.chainCode) ? Buffer.from(arg.chainCode, 'hex') : arg.chainCode,
    publicKey: _.isString(arg.publicKey) ? Buffer.from(arg.publicKey, 'hex') :
      BufferUtil.isBuffer(arg.publicKey) ? arg.publicKey : arg.publicKey.toBuffer(),
    checksum: _.isNumber(arg.checksum) ? BufferUtil.integerAsBuffer(arg.checksum) : arg.checksum
  };
  return this._buildFromBuffers(buffers);
};

_buildFromSerialized = function (arg) {
  var decoded = Base58Check.decode(arg);
  var buffers = {
    version: decoded.slice(HDPublicKey.VersionStart, HDPublicKey.VersionEnd),
    depth: decoded.slice(HDPublicKey.DepthStart, HDPublicKey.DepthEnd),
    parentFingerPrint: decoded.slice(HDPublicKey.ParentFingerPrintStart,
      HDPublicKey.ParentFingerPrintEnd),
    childIndex: decoded.slice(HDPublicKey.ChildIndexStart, HDPublicKey.ChildIndexEnd),
    chainCode: decoded.slice(HDPublicKey.ChainCodeStart, HDPublicKey.ChainCodeEnd),
    publicKey: decoded.slice(HDPublicKey.PublicKeyStart, HDPublicKey.PublicKeyEnd),
    checksum: decoded.slice(HDPublicKey.ChecksumStart, HDPublicKey.ChecksumEnd),
    xpubkey: arg
  };
  return this._buildFromBuffers(buffers);
};

/**
 * Receives a object with buffers in all the properties and populates the
 * internal structure
 *
 * @param {Object} arg
 * @param {buffer.Buffer} arg.version
 * @param {buffer.Buffer} arg.depth
 * @param {buffer.Buffer} arg.parentFingerPrint
 * @param {buffer.Buffer} arg.childIndex
 * @param {buffer.Buffer} arg.chainCode
 * @param {buffer.Buffer} arg.publicKey
 * @param {buffer.Buffer} arg.checksum
 * @param {string=} arg.xpubkey - if set, don't recalculate the base58
 *      representation
 * @return {HDPublicKey} this
 */
_buildFromBuffers = function (arg) {
  /* jshint maxcomplexity: 8 */
  /* jshint maxstatements: 20 */

  this._validateBufferArguments(arg);

  JSUtil.defineImmutable(this, {
    _buffers: arg
  });

  var sequence = [
    arg.version, arg.depth, arg.parentFingerPrint, arg.childIndex, arg.chainCode,
    arg.publicKey
  ];
  var concat = BufferUtil.concat(sequence);
  var checksum = Base58Check.checksum(concat);
  if (!arg.checksum || !arg.checksum.length) {
    arg.checksum = checksum;
  } else {
    if (arg.checksum.toString('hex') !== checksum.toString('hex')) {
      throw new errors.InvalidB58Checksum(concat, checksum);
    }
  }
  var network = Network.get(BufferUtil.integerFromBuffer(arg.version));

  var xpubkey;
  xpubkey = Base58Check.encode(BufferUtil.concat(sequence));
  arg.xpubkey = Buffer.from(xpubkey);

  var publicKey = new PublicKey(arg.publicKey, { network: network });
  var size = HDPublicKey.ParentFingerPrintSize;
  var fingerPrint = Hash.sha256ripemd160(publicKey.toBuffer()).slice(0, size);

  JSUtil.defineImmutable(this, {
    xpubkey: xpubkey,
    network: network,
    depth: BufferUtil.integerFromSingleByteBuffer(arg.depth),
    publicKey: publicKey,
    fingerPrint: fingerPrint
  });

  return this;
};

_validateBufferArguments = function (arg) {
  var checkBuffer = function (name, size) {
    var buff = arg[name];
    assert(BufferUtil.isBuffer(buff), name + ' argument is not a buffer, it\'s ' + typeof buff);
    assert(
      buff.length === size,
      name + ' has not the expected size: found ' + buff.length + ', expected ' + size
    );
  };
  checkBuffer('version', HDPublicKey.VersionSize);
  checkBuffer('depth', HDPublicKey.DepthSize);
  checkBuffer('parentFingerPrint', HDPublicKey.ParentFingerPrintSize);
  checkBuffer('childIndex', HDPublicKey.ChildIndexSize);
  checkBuffer('chainCode', HDPublicKey.ChainCodeSize);
  checkBuffer('publicKey', HDPublicKey.PublicKeySize);
  if (arg.checksum && arg.checksum.length) {
    checkBuffer('checksum', HDPublicKey.CheckSumSize);
  }
};

fromString = function (arg) {
  $.checkArgument(_.isString(arg), 'No valid string was provided');
  return new HDPublicKey(arg);
};

fromObject = function (arg) {
  $.checkArgument(_.isObject(arg), 'No valid argument was provided');
  return new HDPublicKey(arg);
};

/**
 * Returns the base58 checked representation of the public key
 * @return {string} a string starting with "xpub..." in livenet
 */
toString = function () {
  return this.xpubkey;
};

/**
 * Returns the console representation of this extended public key.
 * @return string
 */
inspect = function () {
  return '<HDPublicKey: ' + this.xpubkey + '>';
};

/**
 * Returns a plain JavaScript object with information to reconstruct a key.
 *
 * Fields are: <ul>
 *  <li> network: 'livenet' or 'testnet'
 *  <li> depth: a number from 0 to 255, the depth to the master extended key
 *  <li> fingerPrint: a number of 32 bits taken from the hash of the public key
 *  <li> fingerPrint: a number of 32 bits taken from the hash of this key's
 *  <li>     parent's public key
 *  <li> childIndex: index with which this key was derived
 *  <li> chainCode: string in hexa encoding used for derivation
 *  <li> publicKey: string, hexa encoded, in compressed key format
 *  <li> checksum: BufferUtil.integerFromBuffer(this._buffers.checksum),
 *  <li> xpubkey: the string with the base58 representation of this extended key
 *  <li> checksum: the base58 checksum of xpubkey
 * </ul>
 */
toObject = toJSON = function toObject() {
  return {
    network: Network.get(BufferUtil.integerFromBuffer(this._buffers.version)).name,
    depth: BufferUtil.integerFromSingleByteBuffer(this._buffers.depth),
    fingerPrint: BufferUtil.integerFromBuffer(this.fingerPrint),
    parentFingerPrint: BufferUtil.integerFromBuffer(this._buffers.parentFingerPrint),
    childIndex: BufferUtil.integerFromBuffer(this._buffers.childIndex),
    chainCode: BufferUtil.bufferToHex(this._buffers.chainCode),
    publicKey: this.publicKey.toString(),
    checksum: BufferUtil.integerFromBuffer(this._buffers.checksum),
    xpubkey: this.xpubkey
  };
};

/**
 * Create a HDPublicKey from a buffer argument
 *
 * @param {Buffer} arg
 * @return {HDPublicKey}
 */
fromBuffer = function (arg) {
  return new HDPublicKey(arg);
};

/**
 * Return a buffer representation of the xpubkey
 *
 * @return {Buffer}
 */
toBuffer = function () {
  return BufferUtil.copy(this._buffers.xpubkey);
};


}

assert(HDPublicKey.PublicKeyEnd === HDPublicKey.DataSize);
assert(HDPublicKey.ChecksumEnd === HDPublicKey.SerializedByteSize);
