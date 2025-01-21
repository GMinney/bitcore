'use strict';

var logger = require('../logger');
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;
var Hash = thoughtcore.crypto.Hash;
var $ = thoughtcore.util.preconditions;

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
function Messages(options) {
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

Messages.MINIMUM_LENGTH = 16;
Messages.PAYLOAD_START = 16;
Messages.Message = require('./message');
Messages.builder = require('./builder');

/**
 * @param {Buffers} dataBuffer
 */
Messages.prototype.parseBuffer = function (dataBuffer) {
  /* jshint maxstatements: 18 */

  logger.debug(`Parsing databuffer of length ${dataBuffer.length}`);

  if (dataBuffer.length < Messages.MINIMUM_LENGTH) {
    return;
  }

  logger.debug(`Parsing databuffer 2`);

  // Search the next magic number
  if (!this._discardUntilNextMessage(dataBuffer)) {
    return;
  }

  logger.debug(`Parsing databuffer 3`);

  var payloadLen = (dataBuffer.get(Messages.PAYLOAD_START)) +
    (dataBuffer.get(Messages.PAYLOAD_START + 1) << 8) +
    (dataBuffer.get(Messages.PAYLOAD_START + 2) << 16) +
    (dataBuffer.get(Messages.PAYLOAD_START + 3) << 24);

    logger.debug(`Parsing databuffer 4`);

  var messageLength = 24 + payloadLen;
  if (dataBuffer.length < messageLength) {
    return;
  }

  logger.debug(`Parsing databuffer 5`);

  var command = dataBuffer.slice(4, 16).toString('ascii').replace(/\0+$/, '');
  var payload = dataBuffer.slice(24, messageLength);
  var checksum = dataBuffer.slice(20, 24);

  var checksumConfirm = Hash.sha256sha256(payload).subarray(0, 4);
  if (!BufferUtil.equals(checksumConfirm, checksum)) {
    dataBuffer.skip(messageLength);
    return;
  }

  logger.debug(`Parsing databuffer 6`);

  dataBuffer.skip(messageLength);

  return this._buildFromBuffer(command, payload);
};

Messages.prototype._discardUntilNextMessage = function (dataBuffer) {
  logger.debug(`Parsing Databuffer ... Checking buffer argument against preconditions`);
  $.checkArgument(dataBuffer);
  logger.debug(`Parsing Databuffer ... Checking state of network. Network must be set. Network: ${this.network}`);
  $.checkState(this.network, 'network must be set');
  var i = 0;
  for (; ;) {
    // check if it's the beginning of a new message
    var packageNumber = dataBuffer.slice(0, 4).toString('hex');
    if (packageNumber === this.network.networkMagic.toString('hex')) {
      dataBuffer.skip(i);
      logger.debug(`Parsing Databuffer ... Returning true. Start of message. Network Magic Found: ${this.network.networkMagic.toString('hex')}`);
      return true;
    }

    // did we reach the end of the buffer?
    if (i > (dataBuffer.length - 4)) {
      logger.debug(`Parsing Databuffer ... Reached end of buffer. Returning False.`);
      dataBuffer.skip(i);
      return false;
    }

    i++; // continue scanning
  }
};

Messages.prototype._buildFromBuffer = function (command, payload) {
  if (!this.builder.commands[command]) {
    throw new Error('Unsupported message command: ' + command);
  }
  return this.builder.commands[command].fromBuffer(payload);
};

Messages.prototype.add = function (key, name, Command) {
  this.builder.add(key, Command);
  this[name] = this.builder.commands[key];
};

module.exports = Messages;
