'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var utils = require('../utils');
var BufferUtil = thoughtcore.util.buffer;
var BufferWriter = thoughtcore.encoding.BufferWriter;
var BufferReader = thoughtcore.encoding.BufferReader;
var $ = thoughtcore.util.preconditions;
var _ = thoughtcore.deps._;

/**
 * Request peer to add data to a bloom filter already set by 'filterload'
 * @param {Buffer=} data - Array of bytes representing bloom filter data
 * @param {Object=} options
 * @extends Message
 * @constructor
 */
export class FilteraddMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'filteradd';
    $.checkArgument(
      _.isUndefined(arg) || BufferUtil.isBuffer(arg),
      'First argument is expected to be a Buffer or undefined'
    );
    this.data = arg || BufferUtil.EMPTY_BUFFER;
  }

  setPayload = function (payload) {
    $.checkArgument(payload);
    var parser = new BufferReader(payload);
    this.data = parser.readVarLengthBuffer();
    utils.checkFinished(parser);
  };
  
  getPayload = function () {
    var bw = new BufferWriter();
    bw.writeVarintNum(this.data.length);
    bw.write(this.data);
    return bw.concat();
  };
}

