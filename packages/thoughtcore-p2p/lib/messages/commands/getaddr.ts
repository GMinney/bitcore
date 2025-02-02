'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;

/**
 * Request information about active peers
 * @extends Message
 * @param {Object} options
 * @constructor
 */
export class GetaddrMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'getaddr';
  }

  setPayload = function () { };

  getPayload = function () {
    return BufferUtil.EMPTY_BUFFER;
  };

}

