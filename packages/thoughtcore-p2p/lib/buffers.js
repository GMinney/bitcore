'use strict';

const { Buffer } = require('node:buffer');

class Buffers {
  constructor() {
    this.buffers = [];
    this.length = 0;
  }

  pos(i) {
    // Implement the pos method to calculate the position within the buffers
    let offset = 0;
    for (let bufIndex = 0; bufIndex < this.buffers.length; bufIndex++) {
      const buf = this.buffers[bufIndex];
      if (i < offset + buf.length) {
        return { buf: bufIndex, offset: i - offset };
      }
      offset += buf.length;
    }
    return { buf: this.buffers.length - 1, offset: this.buffers[this.buffers.length - 1].length };
  }
}

Buffers.prototype.skip = function(i) {
  if (i === 0) {
    return;
  }

  if (i >= this.length) {
    this.buffers = [];
    this.length = 0;
    return;
  }

  var pos = this.pos(i);
  this.buffers = this.buffers.slice(pos.buf);
  this.buffers[0] = Buffer.from(this.buffers[0].slice(pos.offset));
  this.length -= i;
};

module.exports = Buffers;

