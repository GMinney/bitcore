'use strict';

import { extend } from "thoughtcore-lib/lib/errors";

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var utils = require('../utils');
var BufferReader = thoughtcore.encoding.BufferReader;
var BufferWriter = thoughtcore.encoding.BufferWriter;

/**
 * @param {Object=} arg
 * @param {Buffer=} arg.payload
 * @param {Buffer=} arg.signature
 * @param {Object} options
 * @extends Message
 * @constructor
 */
export class AlertMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'alert';
    if (!arg) {
      arg = {};
    }
    this.payload = arg.payload || Buffer.from(32);
    this.signature = arg.signature || Buffer.from(32);
  }

  setPayload = function (payload) {
    var parser = new BufferReader(payload);
    this.payload = parser.readVarLengthBuffer();
    this.signature = parser.readVarLengthBuffer();
    utils.checkFinished(parser);
  };
  
  getPayload = function () {
    var bw = new BufferWriter();
    bw.writeVarintNum(this.payload.length);
    bw.write(this.payload);
  
    bw.writeVarintNum(this.signature.length);
    bw.write(this.signature);
  
    return bw.concat();
  };

}