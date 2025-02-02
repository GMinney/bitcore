import _ from "lodash";
import inherits from "inherits";
import Transaction from "../transaction.js";
import Input from "./input.js";
import Output from "../output.js";
import $ from "../../util/preconditions.js";
import Script from "../../script/index.js";
import Signature from "../../crypto/signature.js";
import Sighash from "../sighash.js";
import PublicKey from "../../publickey.js";
import BufferUtil from "../../util/buffer.js";
import TransactionSignature from "../signature.js";

'use strict';

/**
 * @constructor
 */
export class MultiSigInput extends Input {
  threshold: any;
  signatures: any;
  publicKeys: any;
  publicKeyIndex: any;
  output: any;

  static OPCODES_SIZE = 1; // 0
  static SIGNATURE_SIZE = 73; // size (1) + DER (<=72)

  constructor(input, pubkeys, threshold, signatures, opts) {
    super();
    opts = opts || {};
    Input.apply(this, arguments);
    var self = this;
    pubkeys = pubkeys || input.publicKeys;
    threshold = threshold || input.threshold;
    signatures = signatures || input.signatures;
    if (opts.noSorting) {
      this.publicKeys = pubkeys
    } else {
      this.publicKeys = _.sortBy(pubkeys, function (publicKey) { return publicKey.toString('hex'); });
    }
    $.checkState(Script.buildMultisigOut(this.publicKeys, threshold).equals(this.output.script),
      'Provided public keys don\'t match to the provided output script');
    this.publicKeyIndex = {};
    _.each(this.publicKeys, function (publicKey, index) {
      self.publicKeyIndex[publicKey.toString()] = index;
    });
    this.threshold = threshold;
    // Empty array of signatures
    this.signatures = signatures ? this._deserializeSignatures(signatures) : new Array(this.publicKeys.length);
  }
  

toObject = function () {
    var obj = Input.prototype.toObject.apply(this, arguments);
    obj.threshold = this.threshold;
    obj.publicKeys = _.map(this.publicKeys, function (publicKey) { return publicKey.toString(); });
    obj.signatures = this._serializeSignatures();
    return obj;
  };
  
_deserializeSignatures = function (signatures) {
    return _.map(signatures, function (signature) {
      if (!signature) {
        return undefined;
      }
      return new TransactionSignature(signature);
    });
  };
  
_serializeSignatures = function () {
    return _.map(this.signatures, function (signature) {
      if (!signature) {
        return undefined;
      }
      return signature.toObject();
    });
  };
  
  /**
   * Get signatures for this input
   * @param {Transaction} transaction - the transaction to be signed
   * @param {PrivateKey} privateKey - the private key with which to sign the transaction
   * @param {number} index - the index of the input in the transaction input vector
   * @param {number} sigtype - the type of signature, defaults to Signature.SIGHASH_ALL
   * @param {Buffer} hashData - unused for this input type
   * @param {String} signingMethod DEPRECATED - method used to sign - 'ecdsa' or 'schnorr'
   * @param {Buffer} merkleRoot - unused for this input type
   * @return {Array<TransactionSignature>}
   */
getSignatures = function (transaction, privateKey, index, sigtype, hashData, signingMethod, merkleRoot) {
    $.checkState(this.output instanceof Output);
    sigtype = sigtype || this.SIGHASH_ALL;
    signingMethod = signingMethod || 'ecdsa'; // unused. Keeping for consistency with other libs
  
    const results = [];
    for (const publicKey of this.publicKeys || []) {
      if (publicKey.toString() === privateKey.publicKey.toString()) {
        results.push(new TransactionSignature({
          publicKey: privateKey.publicKey,
          prevTxId: this.prevTxId,
          outputIndex: this.outputIndex,
          inputIndex: index,
          signature: Sighash.sign(transaction, privateKey, sigtype, index, this.output.script),
          sigtype: sigtype
        }));
      }
    }
  
    return results;
  };
  
addSignature = function (transaction, signature, signingMethod) {
    $.checkState(!this.isFullySigned(), 'All needed signatures have already been added');
    $.checkArgument(!_.isUndefined(this.publicKeyIndex[signature.publicKey.toString()] + "Signature Undefined"),
      'Signature has no matching public key');
    $.checkState(this.isValidSignature(transaction, signature, signingMethod), "Invalid Signature");
    this.signatures[this.publicKeyIndex[signature.publicKey.toString()]] = signature;
    this._updateScript();
    return this;
  };
  
_updateScript = function () {
    this.setScript(Script.buildMultisigIn(
      this.publicKeys,
      this.threshold,
      this._createSignatures()
    ));
    return this;
  };
  
_createSignatures = function () {
    return _.map(
      _.filter(this.signatures, function (signature) { return !_.isUndefined(signature); }),
      // Future signature types may need refactor of toDER
      function (signature) {
        return BufferUtil.concat([
          signature.signature.toDER(),
          BufferUtil.integerAsSingleByteBuffer(signature.sigtype)
        ]);
      }
    );
  };
  
clearSignatures = function () {
    this.signatures = new Array(this.publicKeys.length);
    this._updateScript();
  };
  
isFullySigned = function () {
    return this.countSignatures() === this.threshold;
  };
  
countMissingSignatures = function () {
    return this.threshold - this.countSignatures();
  };
  
countSignatures = function () {
    return _.reduce(this.signatures, function (sum, signature) {
      return sum + (!!signature);
    }, 0);
  };
  
publicKeysWithoutSignature = function () {
    var self = this;
    return _.filter(this.publicKeys, function (publicKey) {
      return !(self.signatures[self.publicKeyIndex[publicKey.toString()]]);
    });
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
   *
   * @param {Buffer[]} signatures
   * @param {PublicKey[]} publicKeys
   * @param {Transaction} transaction
   * @param {Integer} inputIndex
   * @param {Input} input
   * @param {String} signingMethod DEPRECATED - method used to sign - 'ecdsa' or 'schnorr' (future signing method)
   * @returns {TransactionSignature[]}
   */
normalizeSignatures = function (transaction, input, inputIndex, signatures, publicKeys, signingMethod) {
    signingMethod = signingMethod || 'ecdsa'; // unused. Keeping for consistency with other libs
  
    return publicKeys.map(function (pubKey) {
      var signatureMatch = null;
      signatures = signatures.filter(function (signatureBuffer) {
        if (signatureMatch) {
          return true;
        }
  
        var signature = new TransactionSignature({
          signature: Signature.fromTxFormat(signatureBuffer),
          publicKey: pubKey,
          prevTxId: input.prevTxId,
          outputIndex: input.outputIndex,
          inputIndex: inputIndex,
          sigtype: this.SIGHASH_ALL
        });
  
        signature.signature.nhashtype = signature.sigtype;
        var isMatch = Sighash.verify(
          transaction,
          signature.signature,
          signature.publicKey,
          signature.inputIndex,
          input.output.script
        );
  
        if (isMatch) {
          signatureMatch = signature;
          return false;
        }
  
        return true;
      });
  
      return signatureMatch ? signatureMatch : null;
    });
  };
  

  
_estimateSize = function () {
    return this._getBaseSize() + MultiSigInput.OPCODES_SIZE +
      this.threshold * MultiSigInput.SIGNATURE_SIZE;
  };

}


export default MultiSigInput;
