'use strict';

var Message = require('../message');
var inherits = require('util').inherits;
var thoughtcore = require('thoughtcore-lib');
var BufferWriter = thoughtcore.encoding.BufferWriter;
var BufferReader = thoughtcore.encoding.BufferReader;
var BN = thoughtcore.crypto.BN;
var utils = require('../utils');

/**
 * The version message is used on connection creation to advertise
 * the type of node. The remote node will respond with its version, and no
 * communication is possible until both peers have exchanged their versions.
 *
 * @see https://en.thought.it/wiki/Protocol_documentation#version
 * @param {Object=} arg - properties for the version message
 * @param {Buffer=} arg.nonce - a random 8 byte buffer
 * @param {String=} arg.subversion - version of the client
 * @param {BN=} arg.services
 * @param {Date=} arg.timestamp
 * @param {Number=} arg.startHeight
 * @param {Object} options
 * @extends Message
 * @constructor
 */

function VersionMessage(arg, options) {
  /* jshint maxcomplexity: 10 */
  if (!arg) {
    arg = {};
  }
  Message.call(this, options);
  this.command = 'version';
  this.version = arg.version || options.protocolVersion;
  this.nonce = arg.nonce || utils.getNonce();
  this.services = arg.services || new BN(1, 10);
  this.timestamp = arg.timestamp || new Date();
  this.subversion = arg.subversion || '/Insight:' + "0.18.3" + '/';
  this.startHeight = arg.startHeight || 0;
  this.relay = arg.relay === false ? false : true;
  
}
inherits(VersionMessage, Message);

VersionMessage.prototype.setPayload = function (payload) {
  var parser = new BufferReader(payload);
  this.version = parser.readUInt32LE();
  this.services = parser.readUInt64LEBN();
  this.timestamp = new Date(parser.readUInt64LEBN().toNumber() * 1000);

  this.addrMe = {
    services: parser.readUInt64LEBN(),
    ip: utils.parseIP(parser),
    port: parser.readUInt16BE()
  }; // Shit broken
  this.addrYou = {
    services: parser.readUInt64LEBN(),
    ip: utils.parseIP(parser),
    port: parser.readUInt16BE()
  }; // Shit broken
  this.nonce = parser.read(8);
  this.subversion = parser.readVarLengthBuffer().toString();
  this.startHeight = parser.readUInt32LE();

  if (parser.finished()) {
    this.relay = true;
  } else {
    this.relay = !!parser.readUInt8();
  }
  utils.checkFinished(parser);
};

VersionMessage.prototype.getPayload = function () {
  var bw = new BufferWriter();
  bw.writeUInt32LE(this.version); // Protocol version
  bw.writeUInt64LEBN(this.services); // Services

  var timestampBuffer = Buffer.from(Array(8));
  timestampBuffer.writeUInt32LE(Math.round(this.timestamp.getTime() / 1000), 0);
  bw.write(timestampBuffer); // Time

  utils.writeAddr(this.addrMe, bw); // Remote
  utils.writeAddr(this.addrYou, bw); // Local
  bw.write(this.nonce); // Nonce
  bw.writeVarintNum(this.subversion.length); // Length of User Agent text "'/Insight:' + "0.18.3" + '/'"
  bw.write(Buffer.from(this.subversion, 'ascii')); // The User Agent text "'/Insight:' + "0.18.3" + '/'"
  bw.writeUInt32LE(this.startHeight); // Last block or start height
  bw.writeUInt8(this.relay); // Relay transactions (true)?

  return bw.concat();
};


// For every message, this is the structure:
//
//  Header:  F9BEB4D976657273696F6E0000000000550000002C2F86F3
//  Payload: 7E1101000000000000000000C515CF6100000000000000000000000000000000000000000000FFFF2E13894A208D000000000000000000000000000000000000FFFF7F000001208D00000000000000000000000000
//
// Header: (version message)
// ┌─────────────┬──────────────┬───────────────┬───────┬─────────────────────────────────────┐
// │ Name        │ Example Data │ Format        │ Size  │ Bytes                               │
// ├─────────────┼──────────────┼───────────────┼───────┼─────────────────────────────────────┤
// │ Magic Bytes │              │ bytes         │     4 │ F9 BE B4 D9                         │
// │ Command     │ "version"    │ ascii bytes   │    12 │ 76 65 72 73 69 6F 6E 00 00 00 00 00 │
// │ Size        │ 85           │ little-endian │     4 │ 55 00 00 00                         │
// │ Checksum    │              │ bytes         │     4 │ F7 63 9C 60                         │
// └─────────────┴──────────────┴───────────────┴───────┴─────────────────────────────────────┘
//
// Payload (version message):
// ┌───────────────────────┬─────────────────────┬────────────────────────────┬─────────┬─────────────────────────────────────────────────┐
// │ Name                  │ Example Data        │ Format                     │    Size │ Example Bytes                                   │
// ├───────────────────────┼─────────────────────┼────────────────────────────┼─────────┼─────────────────────────────────────────────────┤
// │ Protocol Version      │ 70014               │ little-endian              │       4 │ 7E 11 01 00                                     │
// │ Services              │ 0                   │ bit field, little-endian   │       8 │ 00 00 00 00 00 00 00 00                         │
// │ Time                  │ 1640961477          │ little-endian              │       8 │ C5 15 CF 61 00 00 00 00                         │
// │ Remote Services       │ 0                   │ bit field, little-endian   │       8 │ 00 00 00 00 00 00 00 00                         │
// │ Remote IP             │ 46.19.137.74        │ ipv6, big-endian           │      16 │ 00 00 00 00 00 00 00 00 00 00 FF FF 2E 13 89 4A │
// │ Remote Port           │ 8333                │ big-endian                 │       2 │ 20 8D                                           │
// │ Local Services        │ 0                   │ bit field, little-endian   │       8 │ 00 00 00 00 00 00 00 00                         │
// │ local IP              │ 127.0.0.1           │ ipv6, big-endian           │      16 │ 00 00 00 00 00 00 00 00 00 00 FF FF 7F 00 00 01 │
// │ Local Port            │ 8333                │ big-endian                 │       2 │ 20 8D                                           │
// │ Nonce                 │ 0                   │ little-endian              │       8 │ 00 00 00 00 00 00 00 00                         │
// │ User Agent            │ ""                  │ compact size, ascii        │ compact │ 00                                              │
// │ Last Block            │ 0                   │ little-endian              │       4 │ 00 00 00 00                                     │
// └───────────────────────┴─────────────────────┴────────────────────────────┴─────────┴─────────────────────────────────────────────────┘

// Thoughtcore p2p Format
// ┌───────────────────────┬─────────────────────┬────────────────────────────┬─────────┬─────────────────────────────────────────────────┐
// │ Name                  │ Example Data        │ Format                     │    Size │ Bytes                                           │
// ├───────────────────────┼─────────────────────┼────────────────────────────┼─────────┼─────────────────────────────────────────────────┤
// │ Protocol Version      │ 70018               │ little-endian              │       4 │ 82 11 01 00                                     │
// │ Services              │ 0                   │ bit field, little-endian   │       8 │ 01 00 00 00 00 00 00 00                         │
// │ Time                  │ 1640961477          │ little-endian              │       8 │ 47 6a 88 67 00 00 00 00                         │
// │ Remote Services       │ 0                   │ bit field, little-endian   │       8 │ 00 00 00 00 00 00 00 00                         │
// │ Remote IP             │ 46.19.137.74        │ ipv6, big-endian           │      16 │ 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 │ FF FF Last four are ipv4
// │ Remote Port           │ 8333                │ big-endian                 │       2 │ 00 00                                           │
// │ Local Services        │ 0                   │ bit field, little-endian   │       8 │ 00 00 00 00 00 00 00 00                         │
// │ local IP              │ 127.0.0.1           │ ipv6, big-endian           │      16 │ 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 │ FF FF Last four are ipv4
// │ Local Port            │ 8333                │ big-endian                 │       2 │ 00 00                                           │
// │ Nonce                 │ 0                   │ little-endian              │       8 │ 82 58 8b 05 c3 41 58 c8                         │
// │ User Agent Size       │ 16                  │ little-endian              │       2 │ 10                                              │
// │ User Agent            │ ""                  │ ascii                      │      16?│ 2f 49 6e 73 69 67 68 74 3a 30 2e 31 38 2e 33 2f │
// │ Start/Last Block      │ 0                   │ little-endian              │       4 │ 00 00 00 00                                     │
// │ Transaction Relay     │ 1                   │ little-endian              │       2 │ 01                                              │
// └───────────────────────┴─────────────────────┴────────────────────────────┴─────────┴─────────────────────────────────────────────────┘



module.exports = VersionMessage;
