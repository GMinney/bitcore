import _ from "lodash";
import bs58 from "bs58";
import buffer from "buffer";

'use strict';
const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'.split('');

export class Base58 {
  constructor(obj) {
    if (!(this instanceof Base58)) {
      return new Base58(obj);
    }
    if (Buffer.isBuffer(obj)) {
      var buf = obj;
      this.fromBuffer(buf);
    } else if (typeof obj === 'string') {
      var str = obj;
      this.fromString(str);
    } else if (obj) {
      this.set(obj);
    }
  }

  static validCharacters = function validCharacters(chars) {
    if (buffer.Buffer.isBuffer(chars)) {
      chars = chars.toString();
    }
    return _.every(_.map(chars, function (char) { return _.includes(ALPHABET, char); }));
  };
  
  set = function (obj) {
    this.buf = obj.buf || this.buf || undefined;
    return this;
  };
  
  static encode = function (buf) {
    if (!buffer.Buffer.isBuffer(buf)) {
      throw new Error('Input should be a buffer');
    }
    return bs58.encode(buf);
  };
  
  static decode = function (str) {
    if (typeof str !== 'string') {
      throw new Error('Input should be a string');
    }
    return Buffer.from(bs58.decode(str));
  };
  
  fromBuffer = function (buf) {
    this.buf = buf;
    return this;
  };
  
  fromString = function (str) {
    var buf = Base58.decode(str);
    this.buf = buf;
    return this;
  };
  
  toBuffer = function () {
    return this.buf;
  };
  
  toString = function () {
    return Base58.encode(this.buf);
  };
  

};


export default Base58;
