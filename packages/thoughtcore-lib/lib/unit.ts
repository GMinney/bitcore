import _ from "lodash";
import errors from "./errors/index.js";
import $ from "./util/preconditions.js";

'use strict';



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
export default class Unit {
  _value: any;
  static UNITS = {
    'THT': [1e8, 8],
    'mTHT': [1e5, 5],
    'uTHT': [1e2, 2],
    'bits': [1e2, 2],
    'notions': [1, 0]
  };

  constructor(amount, code) {
    if (!(this instanceof Unit)) {
      return new Unit(amount, code);
    }

    // convert fiat to THT
    if (_.isNumber(code)) {
      if (code <= 0) {
        throw new errors.Unit.InvalidRate(code);
      }
      amount = amount / code;
      code = Unit.UNITS.THT;
    }

    this._value = this._from(amount, code);

    var self = this;
    var defineAccesor = function (key) {
      Object.defineProperty(self, key, {
        get: function () { return self.to(key); },
        enumerable: true,
      });
    };

    Object.keys(Unit.UNITS).forEach(defineAccesor);


    Object.keys(Unit.UNITS).forEach(function (key) {
      Unit[key] = key;
    });
  }


  /**
   * Constructors
   */

  /**
   * Returns a Unit instance created from JSON string or object
   *
   * @param {String|Object} json - JSON with keys: amount and code
   * @returns {Unit} A Unit instance
   */
  fromObject = function fromObject(data) {
    $.checkArgument(_.isObject(data), 'Argument is expected to be an object');
    return new Unit(data.amount, data.code);
  };

  /**
   * Returns a Unit instance created from an amount in THT
   *
   * @param {Number} amount - The amount in THT
   * @returns {Unit} A Unit instance
   */
  fromTHT = function (amount) {
    return new Unit(amount, Unit.UNITS.THT);
  };

  /**
   * Returns a Unit instance created from an amount in mTHT
   *
   * @param {Number} amount - The amount in mTHT
   * @returns {Unit} A Unit instance
   */
  fromMilis = function (amount) {
    return new Unit(amount, Unit.UNITS.mTHT);
  };

  /**
   * Returns a Unit instance created from an amount in bits
   *
   * @param {Number} amount - The amount in bits
   * @returns {Unit} A Unit instance
   */
  fromBits = function (amount) {
    return new Unit(amount, Unit.UNITS.bits);
  };

  /**
   * Returns a Unit instance created from an amount in notions
   *
   * @param {Number} amount - The amount in notions
   * @returns {Unit} A Unit instance
   */
  fromNotions = function (amount) {
    return new Unit(amount, Unit.UNITS.notions);
  };

  /**
   * Returns a Unit instance created from a fiat amount and exchange rate.
   *
   * @param {Number} amount - The amount in fiat
   * @param {Number} rate - The exchange rate THT/fiat
   * @returns {Unit} A Unit instance
   */
  fromFiat = function (amount, rate) {
    return new Unit(amount, rate);
  };

  /**
   * Methods
   */

  _from = function (amount, code) {
    if (!Unit.UNITS[code]) {
      throw new errors.Unit.UnknownCode(code);
    }
    return parseInt((amount * Unit.UNITS[code][0]).toFixed());
  };

  /**
   * Returns the value represented in the specified unit
   *
   * @param {String|Number} code - The unit code or exchange rate
   * @returns {Number} The converted value
   */
  to = function (code) {
    if (_.isNumber(code)) {
      if (code <= 0) {
        throw new errors.Unit.InvalidRate(code);
      }
      return parseFloat((this.THT * code).toFixed(2));
    }

    if (!Unit.UNITS[code]) {
      throw new errors.Unit.UnknownCode(code);
    }

    var value = this._value / Unit.UNITS[code][0];
    return parseFloat(value.toFixed(Unit.UNITS[code][1]));
  };

  /**
   * Returns the value represented in THT
   *
   * @returns {Number} The value converted to THT
   */
  toTHT = function () {
    return this.to(Unit.UNITS.THT);
  };

  /**
   * Returns the value represented in mTHT
   *
   * @returns {Number} The value converted to mTHT
   */
  toMilis = function () {
    return this.to(Unit.UNITS.mTHT);
  };

  /**
   * Returns the value represented in bits
   *
   * @returns {Number} The value converted to bits
   */
  toBits = function () {
    return this.to(Unit.UNITS.bits);
  };

  /**
   * Returns the value represented in notions
   *
   * @returns {Number} The value converted to notions
   */
  toNotions = function () {
    return this.to(Unit.UNITS.notions);
  };

  /**
   * Returns the value represented in fiat
   *
   * @param {string} rate - The exchange rate between THT/currency
   * @returns {Number} The value converted to notions
   */
  atRate = function (rate) {
    return this.to(rate);
  };

  /**
   * Returns a the string representation of the value in notions
   *
   * @returns {string} the value in notions
   */
  toString = function () {
    return this.notions + ' notions';
  };

  /**
   * Returns a plain object representation of the Unit
   *
   * @returns {Object} An object with the keys: amount and code
   */
  toObject = function () {
    return {
      amount: this.THT,
      code: Unit.UNITS.THT
    };
  };


  /**
   * Returns a plain object representation of the Unit
   *
   * @returns {Object} An object with the keys: amount and code
   */
  toJSON = function toObject() {
    return {
      amount: this.THT,
      code: Unit.UNITS.THT
    };
  };

  /**
   * Returns a string formatted for the console
   *
   * @returns {string} the value in notions
   */
  inspect = function () {
    return '<Unit: ' + this.toString() + '>';
  };



}

