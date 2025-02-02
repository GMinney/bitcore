import _ from "lodash";
import $ from "../../util/preconditions.js";
import errors from "../../errors/index.js";
import BufferWriter from "../../encoding/bufferwriter.js";
import BufferUtil from "../../util/buffer.js";
import JSUtil from "../../util/js.js";
import Script from "../../script/index.js";
import Sighash from "../sighash.js";
import Output from "../output.js";

'use strict';
var MAXINT = 0xffffffff; // Math.pow(2, 32) - 1;
var DEFAULT_SEQNUMBER = MAXINT;
var DEFAULT_LOCKTIME_SEQNUMBER = MAXINT - 1;
var DEFAULT_RBF_SEQNUMBER = MAXINT - 2;
const SEQUENCE_LOCKTIME_DISABLE_FLAG = Math.pow(2, 31); // (1 << 31);
const SEQUENCE_LOCKTIME_TYPE_FLAG = Math.pow(2, 22); // (1 << 22);
const SEQUENCE_LOCKTIME_MASK = 0xffff;
const SEQUENCE_LOCKTIME_GRANULARITY = 512; // 512 seconds
const SEQUENCE_BLOCKDIFF_LIMIT = Math.pow(2, 16) - 1; // 16 bits 


export class Input {

  static MAXINT = MAXINT;
  static DEFAULT_SEQNUMBER = DEFAULT_SEQNUMBER;
  static DEFAULT_LOCKTIME_SEQNUMBER = DEFAULT_LOCKTIME_SEQNUMBER;
  static DEFAULT_RBF_SEQNUMBER = DEFAULT_RBF_SEQNUMBER;
  static SEQUENCE_LOCKTIME_TYPE_FLAG = SEQUENCE_LOCKTIME_TYPE_FLAG;

  constructor(params?) {
    if (!(this instanceof Input)) {
      return new Input(params);
    }
    if (params) {
      return this._fromObject(params);
    }
  }

fromObject = function (obj) {
    $.checkArgument(_.isObject(obj));
    var input = new Input();
    return input._fromObject(obj);
  };
  
_fromObject = function (params) {
    var prevTxId;
    if (typeof params.prevTxId === 'string' && JSUtil.isHexa(params.prevTxId)) {
      prevTxId = Buffer.from(params.prevTxId, 'hex');
    } else {
      prevTxId = params.prevTxId;
    }
    this.witnesses = [];
    this.output = params.output ?
      (params.output instanceof Output ? params.output : new Output(params.output)) : undefined;
    this.prevTxId = prevTxId || params.txidbuf;
    this.outputIndex = params.outputIndex == null ? params.txoutnum : params.outputIndex;
    this.sequenceNumber = params.sequenceNumber == null ?
      (params.seqnum == null ? DEFAULT_SEQNUMBER : params.seqnum) : params.sequenceNumber;
    // null script is allowed in setScript()
    if (params.script === undefined && params.scriptBuffer === undefined) {
      throw new errors.Transaction.Input.MissingScript();
    }
    this.setScript(params.scriptBuffer || params.script);
    return this;
  };
  
toObject = toJSON = function toObject() {
    var obj = {
      prevTxId: this.prevTxId.toString('hex'),
      outputIndex: this.outputIndex,
      sequenceNumber: this.sequenceNumber,
      script: this._scriptBuffer.toString('hex'),
    };
    // add human readable form if input contains valid script
    if (this.script) {
      obj.scriptString = this.script.toString();
    }
    if (this.output) {
      obj.output = this.output.toObject();
    }
    return obj;
  };
  
fromBufferReader = function (br) {
    var input = new Input();
    input.prevTxId = br.readReverse(32);
    input.outputIndex = br.readUInt32LE();
    input._scriptBuffer = br.readVarLengthBuffer();
    input.sequenceNumber = br.readUInt32LE();
    // TODO: return different classes according to which input it is
    // e.g: CoinbaseInput, PublicKeyHashInput, MultiSigScriptHashInput, etc.
    return input;
  };
  
toBufferWriter = function (writer) {
    if (!writer) {
      writer = new BufferWriter();
    }
    writer.writeReverse(this.prevTxId);
    writer.writeUInt32LE(this.outputIndex);
    var script = this._scriptBuffer;
    writer.writeVarintNum(script.length);
    writer.write(script);
    writer.writeUInt32LE(this.sequenceNumber);
    return writer;
  };
  
setScript = function (script) {
    this._script = null;
    if (script instanceof Script) {
      this._script = script;
      this._script._isInput = true;
      this._scriptBuffer = script.toBuffer();
    } else if (JSUtil.isHexa(script)) {
      // hex string script
      this._scriptBuffer = Buffer.from(script, 'hex');
    } else if (_.isString(script)) {
      // human readable string script
      this._script = new Script(script);
      this._script._isInput = true;
      this._scriptBuffer = this._script.toBuffer();
    } else if (BufferUtil.isBuffer(script)) {
      // buffer script
      this._scriptBuffer = Buffer.from(script);
    } else {
      throw new TypeError('Invalid argument type: script');
    }
    return this;
  };
  
  /**
   * Retrieve signatures for the provided PrivateKey.
   *
   * @param {Transaction} transaction - the transaction to be signed
   * @param {PrivateKey} privateKey - the private key to use when signing
   * @param {number} inputIndex - the index of this input in the provided transaction
   * @param {number} sigType - defaults to Signature.SIGHASH_ALL
   * @param {Buffer} addressHash - if provided, don't calculate the hash of the
   *     public key associated with the private key provided
   * @abstract
   */
getSignatures = function () {
    throw new errors.AbstractMethodInvoked(
      'Trying to sign unsupported output type (only P2PKH and P2SH multisig inputs are supported)' +
      ' for input: ' + JSON.stringify(this)
    );
  };
  
getNotionsBuffer = function () {
    $.checkState(this.output instanceof Output);
    $.checkState(this.output._notionsBN);
    return new BufferWriter().writeUInt64LEBN(this.output._notionsBN).toBuffer();
  };
  
  
isFullySigned = function () {
    throw new errors.AbstractMethodInvoked('Input#isFullySigned');
  };
  
isFinal = function () {
    return this.sequenceNumber !== Input.MAXINT;
  };
  
addSignature = function () {
    throw new errors.AbstractMethodInvoked('Input#addSignature');
  };
  
clearSignatures = function () {
    throw new errors.AbstractMethodInvoked('Input#clearSignatures');
  };
  
hasWitnesses = function () {
    if (this.witnesses && this.witnesses.length > 0) {
      return true;
    }
    return false;
  };
  
getWitnesses = function () {
    return this.witnesses;
  };
  
setWitnesses = function (witnesses) {
    this.witnesses = witnesses;
  };
  
isValidSignature = function (transaction, signature, signingMethod) {
    signingMethod = signingMethod || 'ecdsa'; // unused. Keeping for consistency with other libs
    // FIXME: Refactor signature so this is not necessary
    signature.signature.nhashtype = signature.sigtype;
    return Sighash.verify(
      transaction,
      signature.signature,
      signature.publicKey,
      signature.inputIndex,
      this.output.script
    );
  };
  
  /**
   * @returns true if this is a coinbase input (represents no input)
   */
isNull = function () {
    return this.prevTxId.toString('hex') === '0000000000000000000000000000000000000000000000000000000000000000' &&
      this.outputIndex === 0xffffffff;
  };
  
_estimateSize = function () {
    return this.toBufferWriter().toBuffer().length;
  };
  
_getBaseSize = function () {
    return 32 + 4 + 4; // outpoint (32 + 4) + sequence (4)
  };
  
  
  /**
   * Sets sequence number so that transaction is not valid until the desired seconds
   *  since the transaction is mined
   *
   * @param {Number} time in seconds
   * @return {Transaction} this
   */
lockForSeconds = function (seconds) {
    $.checkArgument(_.isNumber(seconds));
    if (seconds < 0 || seconds >= SEQUENCE_LOCKTIME_GRANULARITY * SEQUENCE_LOCKTIME_MASK) {
      throw new errors.Transaction.Input.LockTimeRange();
    }
    seconds = Math.floor(seconds / SEQUENCE_LOCKTIME_GRANULARITY);
  
    // SEQUENCE_LOCKTIME_DISABLE_FLAG = 1 
    this.sequenceNumber = seconds | SEQUENCE_LOCKTIME_TYPE_FLAG;
    return this;
  };
  
  /**
   * Sets sequence number so that transaction is not valid until the desired block height differnece since the tx is mined
   *
   * @param {Number} height
   * @return {Transaction} this
   */
lockUntilBlockHeight = function (heightDiff) {
    $.checkArgument(_.isNumber(heightDiff));
    if (heightDiff < 0 || heightDiff >= SEQUENCE_BLOCKDIFF_LIMIT) {
      throw new errors.Transaction.Input.BlockHeightOutOfRange();
    }
    // SEQUENCE_LOCKTIME_TYPE_FLAG = 0
    // SEQUENCE_LOCKTIME_DISABLE_FLAG = 0
    this.sequenceNumber = heightDiff;
    return this;
  };
  
  
  /**
   *  Returns a semantic version of the input's sequence nLockTime.
   *  @return {Number|Date}
   *  If sequence lock is disabled  it returns null,
   *  if is set to block height lock, returns a block height (number)
   *  else it returns a Date object.
   */
getLockTime = function () {
    if (this.sequenceNumber & SEQUENCE_LOCKTIME_DISABLE_FLAG) {
      return null;
    }
  
    if (this.sequenceNumber & SEQUENCE_LOCKTIME_TYPE_FLAG) {
      var seconds = SEQUENCE_LOCKTIME_GRANULARITY * (this.sequenceNumber & SEQUENCE_LOCKTIME_MASK);
      return seconds;
    } else {
      var blockHeight = this.sequenceNumber & SEQUENCE_LOCKTIME_MASK;
      return blockHeight;
    }
  };
  
}



Object.defineProperty(Input.prototype, 'script', {
  configurable: false,
  enumerable: true,
  get: function () {
    if (this.isNull()) {
      return null;
    }
    if (!this._script) {
      this._script = new Script(this._scriptBuffer);
      this._script._isInput = true;
    }
    return this._script;
  }
});

