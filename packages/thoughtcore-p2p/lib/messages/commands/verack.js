'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferUtil = thoughtcore.util.buffer;

/**
 * A message in response to a version message.
 * @extends Message
 * @constructor
 */
function VerackMessage(arg, options) {
  if (!arg) {
    arg = {};
  }
  Message.call(this, options);
  this.command = 'verack';
}
inherits(VerackMessage, Message);

VerackMessage.prototype.setPayload = function (payload) { };

VerackMessage.prototype.getPayload = function () {
  return BufferUtil.EMPTY_BUFFER;
};

// For a verack message, this is the structure:
//
//  Header:  F9BEB4D976657261636B000000000000000000005DF6E0E2
//  (No payload)
//
// Verack Message:
// ┌─────────────┬──────────────┬───────────────┬───────┬─────────────────────────────────────┐
// │ Name        │ Example Data │ Format        │ Size  │ Example Bytes                       │
// ├─────────────┼──────────────┼───────────────┼───────┼─────────────────────────────────────┤
// │ Magic Bytes │              │ bytes         │     4 │ F9 BE B4 D9                         │
// │ Command     │ "verack"     │ ascii bytes   │    12 │ 76 65 72 61 63 6B 00 00 00 00 00 00 │
// │ Size        │ 0            │ little-endian │     0 │ 00 00 00 00                         │
// │ Checksum    │              │ bytes         │     4 │ 5D F6 E0 E2                         │
// └─────────────┴──────────────┴───────────────┴───────┴─────────────────────────────────────┘



module.exports = VerackMessage;
