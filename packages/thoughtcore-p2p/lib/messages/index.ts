'use strict';

import thoughtcore from 'thoughtcore-lib';
import BufferUtil from 'thoughtcore-lib';
import Hash from 'thoughtcore-lib';
import Preconditions from 'thoughtcore-lib'; // as $
import { logger } from './../logger';

/**
 * A factory to build Thought protocol messages.
 * @param {Object=} options
 * @param {Network=} options.network
 * @param {Function=} options.Block - A block constructor
 * @param {Function=} options.BlockHeader - A block header constructor
 * @param {Function=} options.MerkleBlock - A merkle block constructor
 * @param {Function=} options.Transaction - A transaction constructor
 * @constructor
 */
export class Messages {
  builder: any;
  network: any;
  static MINIMUM_LENGTH = 24;
  static PAYLOAD_START = 16;

  constructor(options) {
    this.builder = Messages.builder(options);

    // map message constructors by name
    for (var key in this.builder.commandsMap) {
      var name = this.builder.commandsMap[key];
      this[name] = this.builder.commands[key];
    }
  
    if (!options) {
      options = {};
    }
    this.network = options.network || thoughtcore.Networks.defaultNetwork;
  }

  /**
 * @param {Buffers} dataBuffer
 */
parseBuffer = function (dataBuffer) {

  logger.debug(`thoughtcore-p2p index Parsing databuffer of length ${dataBuffer.length}`);
  if (dataBuffer.length < Messages.MINIMUM_LENGTH) {
    return;
  }

  logger.debug(`thoughtcore-p2p index Parsing databuffer 2`);
  // Search the next magic number
  if (!this._discardUntilNextMessage(dataBuffer)) {
    return;
  }

  logger.debug(`thoughtcore-p2p index Parsing databuffer 3`);
  let payloadLen = (dataBuffer.get(Messages.PAYLOAD_START)) +
    (dataBuffer.get(Messages.PAYLOAD_START + 1) << 8) +
    (dataBuffer.get(Messages.PAYLOAD_START + 2) << 16) +
    (dataBuffer.get(Messages.PAYLOAD_START + 3) << 24);

  logger.debug(`thoughtcore-p2p index Parsing databuffer 4`);
  let messageLength = Messages.MINIMUM_LENGTH + payloadLen;
  if (dataBuffer.length < messageLength) {
    return;
  }

  logger.debug(`thoughtcore-p2p index Parsing databuffer 5`);

  let command = dataBuffer.slice(4, 16).toString('ascii').replace(/\0+$/, '');
  let payload = dataBuffer.slice(24, messageLength);
  let checksum = dataBuffer.slice(20, 24);

  let checksumConfirm = Hash.sha256sha256(payload).subarray(0, 4);
  if (!BufferUtil.equals(checksumConfirm, checksum)) {
    dataBuffer.skip(messageLength);
    return;
  }

  logger.debug(`thoughtcore-p2p index Parsing databuffer 6`);

  dataBuffer.skip(messageLength);

  return this._buildFromBuffer(command, payload);
};

_discardUntilNextMessage = function (dataBuffer) {
  logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.1... Checking buffer argument against preconditions`);
  $.checkArgument(dataBuffer);
  logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.2... Checking state of network. Network is set. Network: ${this.network}`);
  $.checkState(this.network, 'network must be set');
  logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.2.1... Checking type of databuffer. ${BufferUtil.isBuffer(dataBuffer)}`);

  try {
    var i = 0;
    // Infinite loop until we find a valid message
    for (; ;) {
      // check if it's the beginning of a new message
      
      var packageNumber = dataBuffer.slice(0, 4).toString('hex');
      logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.3... package number: ${packageNumber}`);
      if (packageNumber === this.network.networkMagic.toString('hex')) {
        dataBuffer.skip(i);
        logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.4... Returning true. Start of message. Network Magic Found: ${this.network.networkMagic.toString('hex')}`);
        return true;
      }
  
      // did we reach the end of the buffer?
      if (i > (dataBuffer.length - 4)) {
        logger.debug(`thoughtcore-p2p index Parsing Databuffer 2.5... Reached end of buffer. Returning False.`);
        dataBuffer.skip(i);
        return false;
      }
  
      i++; // continue scanning
    }
  } catch (err) {
    logger.error(`thoughtcore-p2p index Parsing Databuffer ERROR: ${err}`);
  }
};

_buildFromBuffer = function (command, payload) {
  if (!this.builder.commands[command]) {
    throw new Error('Unsupported message command: ' + command);
  }
  return this.builder.commands[command].fromBuffer(payload);
};

add = function (key, name, Command) {
  this.builder.add(key, Command);
  this[name] = this.builder.commands[key];
};

}

// Messages.Message = require('./message');
// Messages.builder = require('./builder');



