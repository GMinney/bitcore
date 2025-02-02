import _ from "lodash";

'use strict';
/**
 * Determines whether a string contains only hexadecimal values
 *
 * @name JSUtil.isHexa
 * @param {string} value
 * @return {boolean} true if the string is the hexa representation of a number
 */


export default class JSUtil {
  constructor() {}
  static isHexa = function isHexa(value) {
    if (!_.isString(value)) {
      return false;
    }
    return /^[0-9a-fA-F]+$/.test(value);
  };
}