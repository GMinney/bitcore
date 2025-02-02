import inherits from "inherits";
import $ from "../../util/preconditions.js";
import BufferUtil from "../../util/buffer.js";
import Input from "./input.js";
import Output from "../output.js";
import Sighash from "../sighash.js";
import Script from "../../script/index.js";
import Signature from "../../crypto/signature.js";
import TransactionSignature from "../signature.js";

'use strict';

/**
 * Represents a special kind of input of PayToPublicKey kind.
 * @constructor
 */
export class PublicKeyInput extends Input {
  
  static SCRIPT_MAX_SIZE = 73; // sigsize (1 + 72)


  constructor() {
    super();
    Input.apply(this, arguments);
  }


/**
 * @param {Transaction} transaction - the transaction to be signed
 * @param {PrivateKey} privateKey - the private key with which to sign the transaction
 * @param {number} index - the index of the input in the transaction input vector
 * @param {number} sigtype - the type of signature, defaults to Signature.SIGHASH_ALL
 * @param {Buffer} hashData - unused for this input type 
 * @param {String} signingMethod DEPRECATED - method used to sign input - 'ecdsa' or 'schnorr'
 * @return {Array} of objects that can be
 */
getSignatures = function (transaction, privateKey, index, sigtype, hashData, signingMethod) {
  $.checkState(this.output instanceof Output);
  sigtype = sigtype || this.SIGHASH_ALL;
  signingMethod = signingMethod || 'ecdsa'; // unused. Keeping for consistency with other libs
  var publicKey = privateKey.toPublicKey();
  if (publicKey.toString() === this.output.script.getPublicKey().toString('hex')) {
    return [new TransactionSignature({
      publicKey: publicKey,
      prevTxId: this.prevTxId,
      outputIndex: this.outputIndex,
      inputIndex: index,
      signature: Sighash.sign(transaction, privateKey, sigtype, index, this.output.script),
      sigtype: sigtype
    })];
  }
  return [];
};

/**
 * Add the provided signature
 *
 * @param {Object} signature
 * @param {PublicKey} signature.publicKey
 * @param {Signature} signature.signature
 * @param {number=} signature.sigtype
 * @param {String} signingMethod - method used to sign - 'ecdsa' or 'schnorr' (future signing method)
 * @return {PublicKeyInput} this, for chaining
 */
addSignature = function (transaction, signature, signingMethod) {
  $.checkState(this.isValidSignature(transaction, signature, signingMethod), 'Signature is invalid');
  this.setScript(Script.buildPublicKeyIn(
    signature.signature.toDER(),
    signature.sigtype
  ));
  return this;
};

/**
 * Clear the input's signature
 * @return {PublicKeyHashInput} this, for chaining
 */
clearSignatures = function () {
  this.setScript(Script.empty());
  return this;
};

/**
 * Query whether the input is signed
 * @return {boolean}
 */
isFullySigned = function () {
  return this.script.isPublicKeyIn();
};


_estimateSize = function () {
  return this._getBaseSize() + PublicKeyInput.SCRIPT_MAX_SIZE;
};

}

