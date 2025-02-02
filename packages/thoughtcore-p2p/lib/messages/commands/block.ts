'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var $ = thoughtcore.util.preconditions;
var _ = thoughtcore.deps._;

/**
 * @param {Block=} arg - An instance of a Block
 * @param {Object} options
 * @param {Function} options.Block - A block constructor
 * @extends Message
 * @constructor
 */
export class BlockMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.Block = options.Block;
    this.command = 'block';
    $.checkArgument(
      _.isUndefined(arg) || arg instanceof this.Block,
      'An instance of Block or undefined is expected'
    );
    this.block = arg;
  }

  setPayload = function (payload) {
    if (this.Block.prototype.fromRaw) {
      this.block = this.Block.fromRaw(payload);
    } else {
      this.block = this.Block.fromBuffer(payload);
    }
  };
  
  getPayload = function () {
    if (this.Block.prototype.toRaw) {
      return this.block.toRaw();
    }
    return this.block.toBuffer();
  };

}



