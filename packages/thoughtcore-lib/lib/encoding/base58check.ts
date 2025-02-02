import _ from "lodash";
import Base58 from "./base58.js";

'use strict';
//var buffer = require('buffer');
var sha256sha256 = require('../crypto/hash').sha256sha256;

export default class Base58Check {
  constructor(obj) {
    if (!(this instanceof Base58Check))
      return new Base58Check(obj);
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

  set = function (obj) {
    this.buf = obj.buf || this.buf || undefined;
    return this;
  };
  
  static validChecksum = function validChecksum(data, checksum) {
    if (_.isString(data)) {
      data = Buffer.from(Base58.decode(data));
    }
    if (_.isString(checksum)) {
      checksum = Buffer.from(Base58.decode(checksum));
    }
    if (!checksum) {
      checksum = data.slice(-4);
      data = data.slice(0, -4);
    }
    return Base58Check.checksum(data).toString('hex') === checksum.toString('hex');
  };
  
  static decode = function (s) {
    if (typeof s !== 'string')
      throw new Error('Input must be a string');
  
    var buf = Buffer.from(Base58.decode(s));
  
    if (buf.length < 4)
      throw new Error("Input string too short");
  
    var data = buf.subarray(0, -4);
    var csum = buf.subarray(-4);
  
    var hash = sha256sha256(data);
    var hash4 = hash.subarray(0, 4);
  
    if (csum.toString('hex') !== hash4.toString('hex'))
      throw new Error("Checksum mismatch");
  
    return data;
  };
  
  static checksum = function (buffer) {
    return sha256sha256(buffer).subarray(0, 4);
  };
  
  static encode = function (buf) {
    if (!Buffer.isBuffer(buf))
      throw new Error('Input must be a buffer');
    var checkedBuf = Buffer.alloc(buf.length + 4);
    var hash = Base58Check.checksum(buf);
    buf.copy(checkedBuf);
    hash.copy(checkedBuf, buf.length);
    return Base58.encode(checkedBuf);
  };
  
  fromBuffer = function (buf) {
    this.buf = buf;
    return this;
  };
  
  fromString = function (str) {
    var buf = Base58Check.decode(str);
    this.buf = buf;
    return this;
  };
  
  toBuffer = function () {
    return this.buf;
  };
  
  toString = function () {
    return Base58Check.encode(this.buf);
  };
  

};

