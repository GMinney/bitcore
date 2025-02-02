'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;

/**
 * Request peer to clear data for a bloom filter
 * @extends Message
 * @constructor
 */
export class FilterclearMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'filterclear';
  }

  setPayload = function () { };

  getPayload = function () {
    return BufferUtil.EMPTY_BUFFER;
  };
}



