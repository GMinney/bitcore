'use strict';

import thoughtcore from 'thoughtcore-lib';
import BloomFilter from 'bloom-filter';
import BufferReader from 'thoughtcore-lib';
import BufferWriter from 'thoughtcore-lib';

/**
 * A constructor for Bloom Filters
 * @see https://github.com/thoughtnetwork/bloom-filter TODO: just code this library in
 * @param {Buffer} - payload
 */
export function BloomFilter.fromBuffer = function fromBuffer(payload) {
  var obj = {};
  var parser = new BufferReader(payload);
  var length = parser.readVarintNum();
  obj.vData = [];
  for (var i = 0; i < length; i++) {
    obj.vData.push(parser.readUInt8());
  }
  obj.nHashFuncs = parser.readUInt32LE();
  obj.nTweak = parser.readUInt32LE();
  obj.nFlags = parser.readUInt8();
  return new BloomFilter(obj);
};

/**
 * @returns {Buffer}
 */
BloomFilter.prototype.toBuffer = function toBuffer() {
  var bw = new BufferWriter();
  bw.writeVarintNum(this.vData.length);
  for (var i = 0; i < this.vData.length; i++) {
    bw.writeUInt8(this.vData[i]);
  }
  bw.writeUInt32LE(this.nHashFuncs);
  bw.writeUInt32LE(this.nTweak);
  bw.writeUInt8(this.nFlags);
  return bw.concat();
};

