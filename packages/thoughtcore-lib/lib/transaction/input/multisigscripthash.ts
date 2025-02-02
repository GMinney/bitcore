import _ from "lodash";
import inherits from "inherits";
import Input from "./input.js";
import Output from "../output.js";
import $ from "../../util/preconditions.js";
import Address from "../../address.js";
import Script from "../../script/index.js";
import Signature from "../../crypto/signature.js";
import Sighash from "../sighash.js";
import SighashWitness from "../sighashwitness.js";
import BufferWriter from "../../encoding/bufferwriter.js";
import BufferUtil from "../../util/buffer.js";
import TransactionSignature from "../signature.js";

'use strict';

/* jshint maxparams:5 */

/**
 * @constructor
 */
export class MultiSigScriptHashInput extends Input {
  redeemScript: any;
  nestedWitness: any;
  type: any;
  publicKeys: any;
  output: any;
  publicKeyIndex: any;
  threshold: any;
  signatures: any;

  
  static MAX_OPCODES_SIZE = 8; // serialized size (<=3) + 0 .. OP_PUSHDATAx N .. M OP_CHECKMULTISIG
  static MAX_SIGNATURE_SIZE = 74; // size (1) + DER (<=72) + sighash (1)
  static MAX_PUBKEY_SIZE = 34; // size (1) + DER (<=33)
  static REDEEM_SCRIPT_SIZE = 34; // OP_0 (1) + scriptHash (1 + 32)

  constructor(input, pubkeys, threshold, signatures, opts) {
    super();
    opts = opts || {};
    Input.apply(this, arguments);
    pubkeys = pubkeys || input.publicKeys;
    threshold = threshold || input.threshold;
    signatures = signatures || input.signatures;
    if (opts.noSorting) {
      this.publicKeys = pubkeys;
    } else {
      this.publicKeys = _.sortBy(pubkeys, function (publicKey) { return publicKey.toString('hex'); });
    }
    this.redeemScript = Script.buildMultisigOut(this.publicKeys, threshold, opts);
    var nested = Script.buildWitnessMultisigOutFromScript(this.redeemScript);
    if (nested.equals(this.output.script)) {
      this.nestedWitness = false;
      this.type = Address.PayToWitnessScriptHash;
    } else if (Script.buildScriptHashOut(nested).equals(this.output.script)) {
      this.nestedWitness = true;
      this.type = Address.PayToScriptHash;
    } else if (Script.buildScriptHashOut(this.redeemScript).equals(this.output.script)) {
      this.nestedWitness = false;
      this.type = Address.PayToScriptHash;
    } else {
      throw new Error('Provided public keys don\'t hash to the provided output');
    }
  
    if (this.nestedWitness) {
      var scriptSig = new Script();
      scriptSig.add(nested.toBuffer());
      this.setScript(scriptSig);
    }
  
    this.publicKeyIndex = {};
    for (let index = 0; index < this.publicKeys.length; index++) {
      const publicKey = this.publicKeys[index];
      this.publicKeyIndex[publicKey.toString()] = index;
    }
    this.threshold = threshold;
    // Empty array of signatures
    this.signatures = signatures ? this._deserializeSignatures(signatures) : new Array(this.publicKeys.length);
  }
 

toObject = function () {
    var obj = Input.prototype.toObject.apply(this, arguments);
    obj.threshold = this.threshold;
    obj.publicKeys = this.publicKeys.map(function (publicKey) { return publicKey.toString(); });
    obj.signatures = this._serializeSignatures();
    return obj;
  };
  
_deserializeSignatures = function (signatures) {
    return signatures.map(function (signature) {
      if (!signature) {
        return undefined;
      }
      return new TransactionSignature(signature);
    });
  };
  
_serializeSignatures = function () {
    return this.signatures.map(function (signature) {
      if (!signature) {
        return undefined;
      }
      return signature.toObject();
    });
  };
  
getScriptCode = function () {
    var writer = new BufferWriter();
    if (!this.redeemScript.hasCodeseparators()) {
      var redeemScriptBuffer = this.redeemScript.toBuffer();
      writer.writeVarintNum(redeemScriptBuffer.length);
      writer.write(redeemScriptBuffer);
    } else {
      throw new Error('@TODO');
    }
    return writer.toBuffer();
  };
  
getSighash = function (transaction, privateKey, index, sigtype) {
    var hash;
    if (this.nestedWitness || this.type === Address.PayToWitnessScriptHash) {
      var scriptCode = this.getScriptCode();
      var notionsBuffer = this.getNotionsBuffer();
      hash = SighashWitness.sighash(transaction, sigtype, index, scriptCode, notionsBuffer);
    } else {
      hash = Sighash.sighash(transaction, sigtype, index, this.redeemScript);
    }
    return hash;
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
    for (const publicKey of this.publicKeys) {
      if (publicKey.toString() === privateKey.publicKey.toString()) {
        var signature;
        if (this.nestedWitness || this.type === Address.PayToWitnessScriptHash) {
          var scriptCode = this.getScriptCode();
          var notionsBuffer = this.getNotionsBuffer();
          signature = SighashWitness.sign(transaction, privateKey, sigtype, index, scriptCode, notionsBuffer);
        } else {
          signature = Sighash.sign(transaction, privateKey, sigtype, index, this.redeemScript);
        }
        results.push(new TransactionSignature({
          publicKey: privateKey.publicKey,
          prevTxId: this.prevTxId,
          outputIndex: this.outputIndex,
          inputIndex: index,
          signature: signature,
          sigtype: sigtype
        }));
      }
    }
    return results;
  };
  
addSignature = function (transaction, signature, signingMethod) {
    $.checkState(!this.isFullySigned(), 'All needed signatures have already been added');
    $.checkArgument(this.publicKeyIndex[signature.publicKey.toString()] != null,
      'Signature has no matching public key');
    $.checkState(this.isValidSignature(transaction, signature, signingMethod), 'Invalid Signature!');
    this.signatures[this.publicKeyIndex[signature.publicKey.toString()]] = signature;
    this._updateScript();
    return this;
  };
  
_updateScript = function () {
    if (this.nestedWitness || this.type === Address.PayToWitnessScriptHash) {
      var stack = [
        Buffer.alloc(0),
      ];
      var signatures = this._createSignatures();
      for (var i = 0; i < signatures.length; i++) {
        stack.push(signatures[i]);
      }
      stack.push(this.redeemScript.toBuffer());
      this.setWitnesses(stack);
    } else {
      var scriptSig = Script.buildP2SHMultisigIn(
        this.publicKeys,
        this.threshold,
        this._createSignatures(),
        { cachedMultisig: this.redeemScript }
      );
      this.setScript(scriptSig);
    }
    return this;
  };
  
_createSignatures = function () {
    return this.signatures
      .filter(function (signature) { return signature != null; })
      .map(function (signature) {
        return BufferUtil.concat([
          signature.signature.toDER(),
          BufferUtil.integerAsSingleByteBuffer(signature.sigtype)
        ]);
      });
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
    return this.signatures.reduce(function (sum, signature) {
      return sum + (!!signature);
    }, 0);
  };
  
publicKeysWithoutSignature = function () {
    return this.publicKeys.filter((publicKey) => {
      return !(this.signatures[this.publicKeyIndex[publicKey.toString()]]);
    });
  };
  
isValidSignature = function (transaction, signature, signingMethod) {
    signingMethod = signingMethod || 'ecdsa'; // unused. Keeping for consistency with other libs
    if (this.nestedWitness || this.type === Address.PayToWitnessScriptHash) {
      signature.signature.nhashtype = signature.sigtype;
      var scriptCode = this.getScriptCode();
      var notionsBuffer = this.getNotionsBuffer();
      return SighashWitness.verify(
        transaction,
        signature.signature,
        signature.publicKey,
        signature.inputIndex,
        scriptCode,
        notionsBuffer
      );
    } else {
      // FIXME: Refactor signature so this is not necessary
      signature.signature.nhashtype = signature.sigtype;
      return Sighash.verify(
        transaction,
        signature.signature,
        signature.publicKey,
        signature.inputIndex,
        this.redeemScript
      );
    }
  };

  
_estimateSize = function () {
    let result = this._getBaseSize();
    const WITNESS_DISCOUNT = 4;
    const witnessSize = MultiSigScriptHashInput.MAX_OPCODES_SIZE +
      this.threshold * MultiSigScriptHashInput.MAX_SIGNATURE_SIZE +
      this.publicKeys.length * MultiSigScriptHashInput.MAX_PUBKEY_SIZE;
    if (this.type === Address.PayToWitnessScriptHash) {
      result += witnessSize / WITNESS_DISCOUNT;
    } else if (this.nestedWitness) {
      result += witnessSize / WITNESS_DISCOUNT + MultiSigScriptHashInput.REDEEM_SCRIPT_SIZE;
    } else {
      result += witnessSize;
    }
    return result;
  };

}

