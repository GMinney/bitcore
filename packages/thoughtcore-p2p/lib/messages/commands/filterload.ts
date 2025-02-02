'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;
var BloomFilter = require('../../bloomfilter');
var $ = thoughtcore.util.preconditions;
var _ = thoughtcore.deps._;

/**
 * Request peer to send inv messages based on a bloom filter
 * @param {BloomFilter=} arg - An instance of BloomFilter
 * @param {Object} options
 * @extends Message
 * @constructor
 */
export class FilterloadMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'filterload';
    $.checkArgument(
      _.isUndefined(arg) || arg instanceof BloomFilter,
      'An instance of BloomFilter or undefined is expected'
    );
    this.filter = arg;
  }

  setPayload = function (payload) {
    this.filter = BloomFilter.fromBuffer(payload);
  };
  
  getPayload = function () {
    if (this.filter) {
      return this.filter.toBuffer();
    } else {
      return BufferUtil.EMPTY_BUFFER;
    }
  };

}