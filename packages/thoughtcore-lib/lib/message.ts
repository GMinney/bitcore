import _ from "lodash";
import PrivateKey from "./privatekey.js";
import PublicKey from "./publickey.js";
import Address from "./address.js";
import BufferWriter from "./encoding/bufferwriter.js";
import ECDSA from "./crypto/ecdsa.js";
import Signature from "./crypto/signature.js";
import JSUtil from "./util/js.js";
import $ from "./util/preconditions.js";
import Script from "./script/index.js";

'use strict';
var sha256sha256 = require('./crypto/hash').sha256sha256;

export class Message {
  message: any;
  static MAGIC_BYTES: Buffer<ArrayBuffer> = Buffer.from('Thought Signed Message:\n');;
  constructor(message) {
    if (!(this instanceof Message)) {
      return new Message(message);
    }
    $.checkArgument(_.isString(message), 'First argument should be a string');
    this.message = message;

    return this;
  }

  magicHash = function magicHash() {
    var prefix1 = BufferWriter.varintBufNum(Message.MAGIC_BYTES.length);
    var messageBuffer = Buffer.from(this.message);
    var prefix2 = BufferWriter.varintBufNum(messageBuffer.length);
    var buf = Buffer.concat([prefix1, Message.MAGIC_BYTES, prefix2, messageBuffer]);
    var hash = sha256sha256(buf);
    return hash;
  };

  _sign = function _sign(privateKey) {
    $.checkArgument(privateKey instanceof PrivateKey, 'First argument should be an instance of PrivateKey');
    var hash = this.magicHash();
    var ecdsa = new ECDSA();
    ecdsa.hashbuf = hash;
    ecdsa.privkey = privateKey;
    ecdsa.pubkey = privateKey.toPublicKey();
    ecdsa.signRandomK();
    ecdsa.calci();
    return ecdsa.sig;
  };

  /**
   * Will sign a message with a given thought private key.
   *
   * @param {PrivateKey} privateKey - An instance of PrivateKey
   * @returns {String} A base64 encoded compact signature
   */
  sign = function sign(privateKey) {
    var signature = this._sign(privateKey);
    return signature.toCompact().toString('base64');
  };

  _verify = function _verify(publicKey, signature) {
    $.checkArgument(publicKey instanceof PublicKey, 'First argument should be an instance of PublicKey');
    $.checkArgument(signature instanceof Signature, 'Second argument should be an instance of Signature');
    var hash = this.magicHash();
    var verified = ECDSA.verify(hash, signature, publicKey);
    if (!verified) {
      this.error = 'The signature was invalid';
    }
    return verified;
  };

  /**
   * Will return a boolean of the signature is valid for a given thought address.
   * If it isn't the specific reason is accessible via the "error" member.
   *
   * @param {Address|String} thoughtAddress - A thought address
   * @param {String} signatureString - A base64 encoded compact signature
   * @returns {Boolean}
   */
  verify = function verify(thoughtAddress, signatureString) {
    $.checkArgument(thoughtAddress);
    $.checkArgument(signatureString && _.isString(signatureString));

    if (_.isString(thoughtAddress)) {
      thoughtAddress = Address.fromString(thoughtAddress);
    }
    var signature = Signature.fromCompact(Buffer.from(signatureString, 'base64'));

    // recover the public key
    var ecdsa = new ECDSA();
    ecdsa.hashbuf = this.magicHash();
    ecdsa.sig = signature;
    var publicKey = ecdsa.toPublicKey();

    var signatureAddress = Address.fromPublicKey(publicKey, thoughtAddress.network);

    // check that the recovered address and specified address match
    if (thoughtAddress.toString() !== signatureAddress.toString()) {
      this.error = 'The signature did not match the message digest';
      return false;
    }

    return this._verify(publicKey, signature);
  };

  /**
   * Will return a public key string if the provided signature and the message digest is correct
   * If it isn't the specific reason is accessible via the "error" member.
   *
   * @param {Address|String} thoughtAddress - A thought address
   * @param {String} signatureString - A base64 encoded compact signature
   * @returns {String}
   */
  recoverPublicKey = function recoverPublicKey(thoughtAddress, signatureString) {
    $.checkArgument(thoughtAddress);
    $.checkArgument(signatureString && _.isString(signatureString));

    if (_.isString(thoughtAddress)) {
      thoughtAddress = Address.fromString(thoughtAddress);
    }
    var signature = Signature.fromCompact(Buffer.from(signatureString, 'base64'));

    // recover the public key
    var ecdsa = new ECDSA();
    ecdsa.hashbuf = this.magicHash();
    ecdsa.sig = signature;
    var publicKey = ecdsa.toPublicKey();

    var signatureAddress = Address.fromPublicKey(publicKey, thoughtAddress.network);

    // check that the recovered address and specified address match
    if (thoughtAddress.toString() !== signatureAddress.toString()) {
      this.error = 'The signature did not match the message digest';
    }

    return publicKey.toString();
  };

  /**
   * Instantiate a message from a message string
   *
   * @param {String} str - A string of the message
   * @returns {Message} A new instance of a Message
   */
  fromString = function (str) {
    return new Message(str);
  };

  /**
   * Instantiate a message from JSON
   *
   * @param {String} json - An JSON string or Object with keys: message
   * @returns {Message} A new instance of a Message
   */
  fromJSON = function fromJSON(json) {
    if (JSUtil.isValidJSON(json)) {
      json = JSON.parse(json);
    }
    return new Message(json.message);
  };

  /**
   * @returns {Object} A plain object with the message information
   */
  toObject = function toObject() {
    return {
      message: this.message
    };
  };

  /**
   * @returns {String} A JSON representation of the message information
   */
  toJSON = function toJSON() {
    return JSON.stringify(this.toObject());
  };

  /**
   * Will return a the string representation of the message
   *
   * @returns {String} Message
   */
  toString = function () {
    return this.message;
  };

  /**
   * Will return a string formatted for the console
   *
   * @returns {String} Message
   */
  inspect = function () {
    return '<Message: ' + this.toString() + '>';
  };


}
