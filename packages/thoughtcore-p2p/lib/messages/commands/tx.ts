'use strict';

import thoughtcore from 'thoughtcore-lib';
import BufferReader from 'thoughtcore-lib';
import BufferWriter from 'thoughtcore-lib';
import BufferUtil from 'thoughtcore-lib';
import Hash from 'thoughtcore-lib';
import Preconditions from 'thoughtcore-lib'; // as $
import { Message } from '../message'; // as $
import { logger } from './../../logger';
import { utils } from '../utils';
import Deps from 'thoughtcore-lib';

/**
 * @param {Transaction=} arg - An instance of Transaction
 * @param {Object} options
 * @extends Message
 * @constructor
 */
export class TransactionMessage extends Message {
  transaction: Transaction;
  Transaction: Transaction;
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'tx';
    this.Transaction = options.Transaction;
    $.checkArgument(
      _.isUndefined(arg) || arg instanceof this.Transaction,
      'An instance of Transaction or undefined is expected'
    );
    this.transaction = arg;
    if (!this.transaction) {
      this.transaction = new this.Transaction();
    }
  }

  setPayload = function (payload) {
    if (this.Transaction.prototype.fromBuffer) {
      this.transaction = new this.Transaction().fromBuffer(payload);
    } else {
      this.transaction = this.Transaction.fromBuffer(payload);
    }
  };
  
  getPayload = function () {
    return this.transaction.toBuffer();
  };
  

}


