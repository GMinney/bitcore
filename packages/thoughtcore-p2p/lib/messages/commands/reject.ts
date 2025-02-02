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

/**
 * The reject message is sent when messages are rejected.
 *
 * @see https://en.thought.it/wiki/Protocol_documentation#reject
 * @param {Object=} arg - properties for the reject message
 * @param {String=} arg.message - type of message rejected
 * @param {Number=} arg.ccode - code relating to rejected message
 * @param {String=} arg.reason - text version of reason for rejection
 * @param {Buffer=} arg.data - Optional extra data provided by some errors.
 * @param {Object} options
 * @extends Message
 * @constructor
 */
export class RejectMessage extends Message {
  command: string;
  message: string;
  ccode: number;
  reason: string;
  data: Buffer;
  static CCODE = {
    REJECT_MALFORMED: 0x01,
    REJECT_INVALID: 0x10,
    REJECT_OBSOLETE: 0x11,
    REJECT_DUPLICATE: 0x12,
    REJECT_NONSTANDARD: 0x40,
    REJECT_DUST: 0x41,
    REJECT_INSUFFICIENTFEE: 0x42,
    REJECT_CHECKPOINT: 0x43
  };

  constructor(arg, options) {
    super(options);
    if (!arg) {
      arg = {};
    }
    Message.call(this, options);
    this.command = 'reject';
    this.message = arg.message;
    this.ccode = arg.ccode;
    this.reason = arg.reason;
    this.data = arg.data;
    if (!arg) {
      arg = {};
    }
    Message.call(this, options);
    this.command = 'reject';
    this.message = arg.message;
    this.ccode = arg.ccode;
    this.reason = arg.reason;
    this.data = arg.data;
  }

  setPayload = function (payload) {
    var parser = new BufferReader(payload);
    this.message = parser.readVarLengthBuffer().toString('utf-8');
    this.ccode = parser.readUInt8();
    this.reason = parser.readVarLengthBuffer().toString('utf-8');
    this.data = parser.readAll();
    utils.checkFinished(parser);
  };
  
  getPayload = function () {
    var bw = new BufferWriter();
    bw.writeVarintNum(this.message.length);
    bw.write(Buffer.from(this.message, 'utf-8'));
    bw.writeUInt8(this.ccode);
    bw.writeVarintNum(this.reason.length);
    bw.write(Buffer.from(this.reason, 'utf-8'));
    bw.write(this.data);
    return bw.toBuffer();
  };
  
}
