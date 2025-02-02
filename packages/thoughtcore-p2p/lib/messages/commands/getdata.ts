'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var utils = require('../utils');
var BufferReader = thoughtcore.encoding.BufferReader;
var BufferWriter = thoughtcore.encoding.BufferWriter;
var _ = thoughtcore.deps._;

/**
 * @param {Object|Array=} - options - If options is an array will use as "inventory"
 * @param {Array=} options.inventory - An array of inventory items
 * @extends Message
 * @constructor
 */
export class GetdataMessage extends Message {
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'getdata';
    utils.checkInventory(arg);
    this.inventory = arg;
  }

  setPayload = function (payload) {
    this.inventory = [];
  
    var parser = new BufferReader(payload);
    var count = parser.readVarintNum();
    for (var i = 0; i < count; i++) {
      var type = parser.readUInt32LE();
      var hash = parser.read(32);
      this.inventory.push({ type: type, hash: hash });
    }
  
    utils.checkFinished(parser);
  };
  
  getPayload = function () {
    var bw = new BufferWriter();
    utils.writeInventory(this.inventory, bw);
    return bw.concat();
  };
  
}
