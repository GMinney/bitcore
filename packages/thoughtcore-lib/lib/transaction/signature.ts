import $ from "../util/preconditions.js";
import inherits from "inherits";
import BufferUtil from "../util/buffer.js";
import JSUtil from "../util/js.js";
import PublicKey from "../publickey.js";
import errors from "../errors/index.js";
import Signature from "../crypto/signature.js";

'use strict';

/**
 * @desc
 * Wrapper around Signature with fields related to signing a transaction specifically
 *
 * @param {Object|string|TransactionSignature} arg
 * @constructor
 */
export class TransactionSignature extends Signature {
  constructor(arg) {
    super(arg)
    if (!(this instanceof TransactionSignature)) {
      return new TransactionSignature(arg);
    }
    if (arg instanceof TransactionSignature) {
      return arg;
    }
    if (arg && typeof arg === 'object') {
      return this._fromObject(arg);
    }
    throw new errors.InvalidArgument('TransactionSignatures must be instantiated from an object');
  }

  
_fromObject = function (arg) {
  this._checkObjectArgs(arg);
  this.publicKey = new PublicKey(arg.publicKey);
  this.prevTxId = BufferUtil.isBuffer(arg.prevTxId) ? arg.prevTxId : Buffer.from(arg.prevTxId, 'hex');
  this.outputIndex = arg.outputIndex;
  this.inputIndex = arg.inputIndex;
  this.signature = (arg.signature instanceof Signature) ? arg.signature :
    BufferUtil.isBuffer(arg.signature) ? Signature.fromBuffer(arg.signature) :
      Signature.fromString(arg.signature);
  this.sigtype = arg.sigtype;
  return this;
};

_checkObjectArgs = function (arg) {
  $.checkArgument(PublicKey(arg.publicKey), 'invalid publicKey');
  $.checkArgument(arg.inputIndex != null, 'missing inputIndex');
  $.checkArgument(arg.outputIndex != null, 'missing outputIndex');
  $.checkState(!isNaN(arg.inputIndex), 'inputIndex must be a number');
  $.checkState(!isNaN(arg.outputIndex), 'outputIndex must be a number');
  $.checkArgument(arg.signature, 'missing signature');
  $.checkArgument(arg.prevTxId, 'missing prevTxId');
  $.checkState(arg.signature instanceof Signature ||
    BufferUtil.isBuffer(arg.signature) ||
    JSUtil.isHexa(arg.signature), 'signature must be a buffer or hexa value');
  $.checkState(BufferUtil.isBuffer(arg.prevTxId) ||
    JSUtil.isHexa(arg.prevTxId), 'prevTxId must be a buffer or hexa value');
  $.checkArgument(arg.sigtype != null, 'missing sigtype');
  $.checkState(!isNaN(arg.sigtype), 'sigtype must be a number');
};

/**
 * Serializes a transaction to a plain JS object
 * @return {Object}
 */
toObject = toJSON = function toObject() {
  return {
    publicKey: this.publicKey.toString(),
    prevTxId: this.prevTxId.toString('hex'),
    outputIndex: this.outputIndex,
    inputIndex: this.inputIndex,
    signature: this.signature.toString(),
    sigtype: this.sigtype
  };
};

/**
 * Builds a TransactionSignature from an object
 * @param {Object} object
 * @return {TransactionSignature}
 */
fromObject = function (object) {
  $.checkArgument(object);
  return new TransactionSignature(object);
};

}
