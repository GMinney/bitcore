'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;
var $ = thoughtcore.util.preconditions;
var _ = thoughtcore.deps._;

/**
 * Contains information about a MerkleBlock
 * @see https://en.thought.it/wiki/Protocol_documentation
 * @param {MerkleBlock} arg - An instance of MerkleBlock
 * @param {Object=} options
 * @param {Function} options.MerkleBlock - a MerkleBlock constructor
 * @extends Message
 * @constructor
 */
export class MerkleblockMessage extends Message {
  command: string;
  merkleBlock: MerkleBlock;
  MerkleBlock: MerkleBlock;
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.MerkleBlock = options.MerkleBlock; // constructor
    this.command = 'merkleblock';
    $.checkArgument(
      _.isUndefined(arg) || arg instanceof this.MerkleBlock,
      'An instance of MerkleBlock or undefined is expected'
    );
    this.merkleBlock = arg;

  }

  setPayload = function (payload) {
    $.checkArgument(BufferUtil.isBuffer(payload));
    this.merkleBlock = this.MerkleBlock.fromBuffer(payload);
  };

  getPayload = function () {
    return this.merkleBlock ? this.merkleBlock.toBuffer() : BufferUtil.EMPTY_BUFFER;
  };

}
