'use strict';

import thoughtcore from 'thoughtcore-lib';
import BufferReader from 'thoughtcore-lib';
import BufferWriter from 'thoughtcore-lib';
import Hash from 'thoughtcore-lib';
import Preconditions from 'thoughtcore-lib'; // as $
import { logger } from './../logger';


/**
 * Base message that can be inherited to add an additional
 * `getPayload` method to modify the message payload.
 * @param {Object=} options
 * @param {String=} options.command
 * @param {Network=} options.network
 * @constructor
 */
export class Message {
  command: string;
  network: any;

  constructor(options) {
    this.command = options.command;
    this.network = options.network;
  }

  /**
   * @returns {Buffer} - Serialized message
   * @constructor
   */
  serialize = function () {
    return this.toBuffer();
  }

  /**
 * @returns {Buffer} - Serialized message
 * @constructor
 */
toBuffer = function () {
  logger.debug(`thoughtcore-p2p message: Message.prototype.toBuffer, checkState`);
  $.checkState(this.network, 'Need to have a defined network to serialize message');

  logger.debug(`thoughtcore-p2p message: Message.prototype Buffer`);
  var commandBuf = Buffer.alloc(12);

  logger.debug(`thoughtcore-p2p message: Message.prototype Write`);
  commandBuf.write(this.command, 'ascii');

  logger.debug(`thoughtcore-p2p message: Message.prototype getPayload `);
  var payload = this.getPayload();

  logger.debug(`thoughtcore-p2p message: Message.prototype DoubleHash `);
  var checksum = Hash.sha256sha256(payload).subarray(0, 4);

  var bw = new BufferWriter();
  bw.write(this.network.networkMagic);
  bw.write(commandBuf);
  bw.writeUInt32LE(payload.length);
  bw.write(checksum);
  bw.write(payload);

  var bufferMessage =  bw.concat();
  var br = new BufferReader(bufferMessage);
  //var bufferReadout = br.();

  logger.debug(`thoughtcore-p2p message: Message.prototype Buffer contains: ${this.network.networkMagic} | ${commandBuf} | ${payload.length} | ${checksum} | ${payload} | ${bufferMessage} | ${bufferMessage.toString('hex')}`);

  return bufferMessage;
};



}


