import bufferUtil from "../util/buffer.js";
import assert from "assert";

'use strict';
export default class BufferWriter {
  bufs: any[];
  bufLen: number;


  constructor(obj?) {
    if (!(this instanceof BufferWriter))
      return new BufferWriter(obj);
    this.bufLen = 0;
    if (obj)
      this.set(obj);
    else
      this.bufs = [];
  }

  set = function (obj) {
    this.bufs = obj.bufs || this.bufs || [];
    this.bufLen = this.bufs.reduce(function (prev, buf) { return prev + buf.length; }, 0);
    return this;
  };
  
  toBuffer = function () {
    return this.concat();
  };
  
  concat = function () {
    return Buffer.concat(this.bufs, this.bufLen);
  };
  
  write = function (buf) {
    assert(bufferUtil.isBuffer(buf));
    this.bufs.push(buf);
    this.bufLen += buf.length;
    return this;
  };
  
  writeReverse = function (buf) {
    assert(bufferUtil.isBuffer(buf));
    this.bufs.push(bufferUtil.reverse(buf));
    this.bufLen += buf.length;
    return this;
  };
  
  writeUInt8 = function (n) {
    if (n < 0) {
      n = n >>> 0; // Convert signed int to unsigned int
    }
    var buf = Buffer.alloc(1);
    buf.writeUInt8(n, 0);
    this.write(buf);
    return this;
  };
  
  writeUInt16BE = function (n) {
    if (n < 0) {
      n = n >>> 0; // Convert signed int to unsigned int
    }
    var buf = Buffer.alloc(2);
    buf.writeUInt16BE(n, 0);
    this.write(buf);
    return this;
  };
  
  writeUInt16LE = function (n) {
    if (n < 0) {
      n = n >>> 0; // Convert signed int to unsigned int
    }
    var buf = Buffer.alloc(2);
    buf.writeUInt16LE(n, 0);
    this.write(buf);
    return this;
  };
  
  writeUInt32BE = function (n) {
    if (n < 0) {
      n = n >>> 0; // Convert signed int to unsigned int
    }
    var buf = Buffer.alloc(4);
    buf.writeUInt32BE(n, 0);
    this.write(buf);
    return this;
  };
  
  writeInt32LE = function (n) {
    var buf = Buffer.alloc(4);
    buf.writeInt32LE(n, 0);
    this.write(buf);
    return this;
  };
  
  writeUInt32LE = function (n) {
    if (n < 0) {
      n = n >>> 0; // Convert signed int to unsigned int
    }
    var buf = Buffer.alloc(4);
    buf.writeUInt32LE(n, 0);
    this.write(buf);
    return this;
  };
  
  writeUInt64BEBN = function (bn) {
    var buf = bn.toBuffer({ size: 8 });
    this.write(buf);
    return this;
  };
  
  writeUInt64LEBN = function (bn) {
    var buf = bn.toBuffer({ size: 8 });
    this.writeReverse(buf);
    return this;
  };
  
  writeVarintNum = function (n) {
    var buf = BufferWriter.varintBufNum(n);
    this.write(buf);
    return this;
  };
  
  writeVarintBN = function (bn) {
    var buf = BufferWriter.varintBufBN(bn);
    this.write(buf);
    return this;
  };
  
  static varintBufNum = function (n) {
    var buf = undefined;
    if (n < 253) {
      buf = Buffer.alloc(1);
      buf.writeUInt8(n, 0);
    } else if (n < 0x10000) {
      buf = Buffer.alloc(1 + 2);
      buf.writeUInt8(253, 0);
      buf.writeUInt16LE(n, 1);
    } else if (n < 0x100000000) {
      buf = Buffer.alloc(1 + 4);
      buf.writeUInt8(254, 0);
      buf.writeUInt32LE(n, 1);
    } else {
      buf = Buffer.alloc(1 + 8);
      buf.writeUInt8(255, 0);
      buf.writeInt32LE(n & -1, 1);
      buf.writeUInt32LE(Math.floor(n / 0x100000000), 5);
    }
    return buf;
  };
  
  static varintBufBN = function (bn) {
    var buf = undefined;
    var n = bn.toNumber();
    if (n < 253) {
      buf = Buffer.alloc(1);
      buf.writeUInt8(n, 0);
    } else if (n < 0x10000) {
      buf = Buffer.alloc(1 + 2);
      buf.writeUInt8(253, 0);
      buf.writeUInt16LE(n, 1);
    } else if (n < 0x100000000) {
      buf = Buffer.alloc(1 + 4);
      buf.writeUInt8(254, 0);
      buf.writeUInt32LE(n, 1);
    } else {
      var bw = new BufferWriter();
      bw.writeUInt8(255);
      bw.writeUInt64LEBN(bn);
      var buf = bw.concat();
    }
    return buf;
  };

};

