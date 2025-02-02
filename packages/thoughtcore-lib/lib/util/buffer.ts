import buffer from "buffer";
import $ from "./preconditions.js";

'use strict';

export default class BufferUtil {
  constructor() {}

  static equals(a, b) {
    if (a.length !== b.length) {
      return false;
    }
    var length = a.length;
    for (var i = 0; i < length; i++) {
      if (a[i] !== b[i]) {
        return false;
      }
    }
    return true;
  }

}


export { module.exports.fill(Buffer.alloc(32), 0) };
export { Buffer.alloc(0) };
