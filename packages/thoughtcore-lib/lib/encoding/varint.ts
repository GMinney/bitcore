import BufferWriter from "./bufferwriter.js";
import BufferReader from "./bufferreader.js";
import BN from "../crypto/bn.js";

'use strict';
export class Varint {
  buf: any;

  constructor(buf) {
    if (!(this instanceof Varint))
      return new Varint(buf);
    if (Buffer.isBuffer(buf)) {
      this.buf = buf;
    } else if (typeof buf === 'number') {
      var num = buf;
      this.fromNumber(num);
    } else if (buf instanceof BN) {
      var bn = buf;
      this.fromBN(bn);
    } else if (buf) {
      var obj = buf;
      this.set(obj);
    }
  }

  set = function (obj) {
    this.buf = obj.buf || this.buf;
    return this;
  };
  
  fromString = function (str) {
    this.set({
      buf: Buffer.from(str, 'hex')
    });
    return this;
  };
  
  toString = function () {
    return this.buf.toString('hex');
  };
  
  fromBuffer = function (buf) {
    this.buf = buf;
    return this;
  };
  
  fromBufferReader = function (br) {
    this.buf = br.readVarintBuf();
    return this;
  };
  
  fromBN = function (bn) {
    this.buf = BufferWriter().writeVarintBN(bn).concat();
    return this;
  };
  
  fromNumber = function (num) {
    this.buf = BufferWriter().writeVarintNum(num).concat();
    return this;
  };
  
  toBuffer = function () {
    return this.buf;
  };
  
  toBN = function () {
    return BufferReader(this.buf).readVarintBN();
  };
  
  toNumber = function () {
    return BufferReader(this.buf).readVarintNum();
  };

};
