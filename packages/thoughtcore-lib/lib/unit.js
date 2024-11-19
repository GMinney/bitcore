'use strict';

var _ = require('lodash');

var errors = require('./errors');
var $ = require('./util/preconditions');

var UNITS = {
  'THT': [1e8, 8],
  'mTHT': [1e5, 5],
  'uTHT': [1e2, 2],
  'bits': [1e2, 2],
  'notions': [1, 0]
};

/**
 * Utility for handling and converting thoughts units. The supported units are
 * THT, mTHT, bits (also named uTHT) and notions. A unit instance can be created with an
 * amount and a unit code, or alternatively using static methods like {fromTHT}.
 * It also allows to be created from a fiat amount and the exchange rate, or
 * alternatively using the {fromFiat} static method.
 * You can consult for different representation of a unit instance using it's
 * {to} method, the fixed unit methods like {toNotions} or alternatively using
 * the unit accessors. It also can be converted to a fiat amount by providing the
 * corresponding THT/fiat exchange rate.
 *
 * @example
 * ```javascript
 * var sats = Unit.fromTHT(1.3).toNotions();
 * var mili = Unit.fromBits(1.3).to(Unit.mTHT);
 * var bits = Unit.fromFiat(1.3, 350).bits;
 * var tht = new Unit(1.3, Unit.bits).THT;
 * ```
 *
 * @param {Number} amount - The amount to be represented
 * @param {String|Number} code - The unit of the amount or the exchange rate
 * @returns {Unit} A new instance of an Unit
 * @constructor
 */
function Unit(amount, code) {
  if (!(this instanceof Unit)) {
    return new Unit(amount, code);
  }

  // convert fiat to THT
  if (_.isNumber(code)) {
    if (code <= 0) {
      throw new errors.Unit.InvalidRate(code);
    }
    amount = amount / code;
    code = Unit.THT;
  }

  this._value = this._from(amount, code);

  var self = this;
  var defineAccesor = function (key) {
    Object.defineProperty(self, key, {
      get: function () { return self.to(key); },
      enumerable: true,
    });
  };

  Object.keys(UNITS).forEach(defineAccesor);
}

Object.keys(UNITS).forEach(function (key) {
  Unit[key] = key;
});

/**
 * Returns a Unit instance created from JSON string or object
 *
 * @param {String|Object} json - JSON with keys: amount and code
 * @returns {Unit} A Unit instance
 */
Unit.fromObject = function fromObject(data) {
  $.checkArgument(_.isObject(data), 'Argument is expected to be an object');
  return new Unit(data.amount, data.code);
};

/**
 * Returns a Unit instance created from an amount in THT
 *
 * @param {Number} amount - The amount in THT
 * @returns {Unit} A Unit instance
 */
Unit.fromTHT = function (amount) {
  return new Unit(amount, Unit.THT);
};

/**
 * Returns a Unit instance created from an amount in mTHT
 *
 * @param {Number} amount - The amount in mTHT
 * @returns {Unit} A Unit instance
 */
Unit.fromMillis = Unit.fromMilis = function (amount) {
  return new Unit(amount, Unit.mTHT);
};

/**
 * Returns a Unit instance created from an amount in bits
 *
 * @param {Number} amount - The amount in bits
 * @returns {Unit} A Unit instance
 */
Unit.fromMicros = Unit.fromBits = function (amount) {
  return new Unit(amount, Unit.bits);
};

/**
 * Returns a Unit instance created from an amount in notions
 *
 * @param {Number} amount - The amount in notions
 * @returns {Unit} A Unit instance
 */
Unit.fromNotions = function (amount) {
  return new Unit(amount, Unit.notions);
};

/**
 * Returns a Unit instance created from a fiat amount and exchange rate.
 *
 * @param {Number} amount - The amount in fiat
 * @param {Number} rate - The exchange rate THT/fiat
 * @returns {Unit} A Unit instance
 */
Unit.fromFiat = function (amount, rate) {
  return new Unit(amount, rate);
};

Unit.prototype._from = function (amount, code) {
  if (!UNITS[code]) {
    throw new errors.Unit.UnknownCode(code);
  }
  return parseInt((amount * UNITS[code][0]).toFixed());
};

/**
 * Returns the value represented in the specified unit
 *
 * @param {String|Number} code - The unit code or exchange rate
 * @returns {Number} The converted value
 */
Unit.prototype.to = function (code) {
  if (_.isNumber(code)) {
    if (code <= 0) {
      throw new errors.Unit.InvalidRate(code);
    }
    return parseFloat((this.THT * code).toFixed(2));
  }

  if (!UNITS[code]) {
    throw new errors.Unit.UnknownCode(code);
  }

  var value = this._value / UNITS[code][0];
  return parseFloat(value.toFixed(UNITS[code][1]));
};

/**
 * Returns the value represented in THT
 *
 * @returns {Number} The value converted to THT
 */
Unit.prototype.toTHT = function () {
  return this.to(Unit.THT);
};

/**
 * Returns the value represented in mTHT
 *
 * @returns {Number} The value converted to mTHT
 */
Unit.prototype.toMillis = Unit.prototype.toMilis = function () {
  return this.to(Unit.mTHT);
};

/**
 * Returns the value represented in bits
 *
 * @returns {Number} The value converted to bits
 */
Unit.prototype.toMicros = Unit.prototype.toBits = function () {
  return this.to(Unit.bits);
};

/**
 * Returns the value represented in notions
 *
 * @returns {Number} The value converted to notions
 */
Unit.prototype.toNotions = function () {
  return this.to(Unit.notions);
};

/**
 * Returns the value represented in fiat
 *
 * @param {string} rate - The exchange rate between THT/currency
 * @returns {Number} The value converted to notions
 */
Unit.prototype.atRate = function (rate) {
  return this.to(rate);
};

/**
 * Returns a the string representation of the value in notions
 *
 * @returns {string} the value in notions
 */
Unit.prototype.toString = function () {
  return this.notions + ' notions';
};

/**
 * Returns a plain object representation of the Unit
 *
 * @returns {Object} An object with the keys: amount and code
 */
Unit.prototype.toObject = Unit.prototype.toJSON = function toObject() {
  return {
    amount: this.THT,
    code: Unit.THT
  };
};

/**
 * Returns a string formatted for the console
 *
 * @returns {string} the value in notions
 */
Unit.prototype.inspect = function () {
  return '<Unit: ' + this.toString() + '>';
};

module.exports = Unit;
