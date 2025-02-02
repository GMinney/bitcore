import _ from "lodash";
import $ from "../util/preconditions.js";
import JSUtil from "../util/js.js";
import Script from "../script/index.js";
import Address from "../address.js";
import Unit from "../unit.js";

'use strict';

/**
 * Represents an unspent output information: its script, associated amount and address,
 * transaction id and output index.
 *
 * @constructor
 * @param {object} data
 * @param {string} data.txid the previous transaction id
 * @param {string=} data.txId alias for `txid`
 * @param {number} data.vout the index in the transaction
 * @param {number=} data.outputIndex alias for `vout`
 * @param {string|Script} data.scriptPubKey the script that must be resolved to release the funds
 * @param {string|Script=} data.script alias for `scriptPubKey`
 * @param {number} data.amount amount of thoughts associated
 * @param {number=} data.notions alias for `amount`, but expressed in notions (1 THT = 1e8 notions)
 * @param {string|Address=} data.address the associated address to the script, if provided
 */
export class UnspentOutput {
  outputIndex: any;
  txId: any;
  address: any;
  script: any;
  notions: any;

  constructor(data) {
    if (!(this instanceof UnspentOutput)) {
      return new UnspentOutput(data);
    }
    $.checkArgument(_.isObject(data), 'Must provide an object from where to extract data');
    var address = data.address ? new Address(data.address) : undefined;
    var txId = data.txid ? data.txid : data.txId;
    if (!txId || !JSUtil.isHexaString(txId) || txId.length > 64) {
      // TODO: Use the errors library
      throw new Error('Invalid TXID in object: ' + JSON.stringify(data));
    }
    var outputIndex = _.isUndefined(data.vout) ? data.outputIndex : data.vout;
    if (!_.isNumber(outputIndex)) {
      throw new Error('Invalid outputIndex, received ' + outputIndex);
    }
    $.checkArgument(!_.isUndefined(data.scriptPubKey) || !_.isUndefined(data.script),
      'Must provide the scriptPubKey for that output!');
    var script = new Script(data.scriptPubKey || data.script);
    $.checkArgument(!_.isUndefined(data.amount) || !_.isUndefined(data.notions),
      'Must provide an amount for the output');
    var amount = !_.isUndefined(data.amount) ? new Unit.fromTHT(data.amount).toNotions() : data.notions;
    $.checkArgument(_.isNumber(amount), 'Amount must be a number');
    JSUtil.defineImmutable(this, {
      address: address,
      txId: txId,
      outputIndex: outputIndex,
      script: script,
      notions: amount
    });
  }

  /**
   * Provide an informative output when displaying this object in the console
   * @returns string
   */
  inspect = function () {
    return '<UnspentOutput: ' + this.txId + ':' + this.outputIndex +
      ', notions: ' + this.notions + ', address: ' + this.address + '>';
  };

  /**
   * String representation: just "txid:index"
   * @returns string
   */
  toString = function () {
    return this.txId + ':' + this.outputIndex;
  };

  /**
   * Deserialize an UnspentOutput from an object
   * @param {object|string} data
   * @return UnspentOutput
   */
  fromObject = function (data) {
    return new UnspentOutput(data);
  };

  /**
   * Returns a plain object (no prototype or methods) with the associated info for this output
   * @return {object}
   */
  toObject = toJSON = function toObject() {
    return {
      address: this.address ? this.address.toString() : undefined,
      txid: this.txId,
      vout: this.outputIndex,
      scriptPubKey: this.script.toBuffer().toString('hex'),
      amount: Unit.fromNotions(this.notions).toTHT()
    };
  };


}

