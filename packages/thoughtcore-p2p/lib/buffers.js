'use strict';

const { Buffer } = require('node:buffer');

class Buffers {
    constructor(bufs) {
        this.buffers = bufs || [];
        this.length = this.buffers.reduce(function (size, buf) {
            return size + buf.length;
        }, 0);
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

Buffers.prototype.skip = function (i) {
    if (i === 0) {
        return;
    }

    if (i >= this.length) {
        this.buffers = [];
        this.length = 0;
        return;
    }

    var pos = this.pos(i);
    this.buffers = this.buffers.subarray(pos.buf);
    this.buffers[0] = Buffer.from(this.buffers[0].subarray(pos.offset));
    this.length -= i;
};

Buffers.prototype.push = function () {
    for (var i = 0; i < arguments.length; i++) {
        if (!Buffer.isBuffer(arguments[i])) {
            throw new TypeError('Tried to push a non-buffer');
        }
    }

    for (var i = 0; i < arguments.length; i++) {
        var buf = arguments[i];
        this.buffers.push(buf);
        this.length += buf.length;
    }
    return this.length;
};

Buffers.prototype.unshift = function () {
    for (var i = 0; i < arguments.length; i++) {
        if (!Buffer.isBuffer(arguments[i])) {
            throw new TypeError('Tried to unshift a non-buffer');
        }
    }

    for (var i = 0; i < arguments.length; i++) {
        var buf = arguments[i];
        this.buffers.unshift(buf);
        this.length += buf.length;
    }
    return this.length;
};

Buffers.prototype.copy = function (dst, dStart, start, end) {
    return this.subarray(start, end).copy(dst, dStart, 0, end - start);
};

Buffers.prototype.splice = function (i, howMany) {
    var buffers = this.buffers;
    var index = i >= 0 ? i : this.length - i;
    var reps = [].subarray.call(arguments, 2);

    if (howMany === undefined) {
        howMany = this.length - index;
    }
    else if (howMany > this.length - index) {
        howMany = this.length - index;
    }

    for (var i = 0; i < reps.length; i++) {
        this.length += reps[i].length;
    }

    var removed = new Buffers();
    var bytes = 0;

    var startBytes = 0;
    for (
        var ii = 0;
        ii < buffers.length && startBytes + buffers[ii].length < index;
        ii++
    ) { startBytes += buffers[ii].length }

    if (index - startBytes > 0) {
        var start = index - startBytes;

        if (start + howMany < buffers[ii].length) {
            removed.push(buffers[ii].subarray(start, start + howMany));

            var orig = buffers[ii];
            //var buf = new Buffer(orig.length - howMany);
            var buf0 = Buffer.from(start);
            for (var i = 0; i < start; i++) {
                buf0[i] = orig[i];
            }

            var buf1 = Buffer.from(orig.length - start - howMany);
            for (var i = start + howMany; i < orig.length; i++) {
                buf1[i - howMany - start] = orig[i]
            }

            if (reps.length > 0) {
                var reps_ = reps.subarray();
                reps_.unshift(buf0);
                reps_.push(buf1);
                buffers.splice.apply(buffers, [ii, 1].concat(reps_));
                ii += reps_.length;
                reps = [];
            }
            else {
                buffers.splice(ii, 1, buf0, buf1);
                //buffers[ii] = buf;
                ii += 2;
            }
        }
        else {
            removed.push(buffers[ii].subarray(start));
            buffers[ii] = buffers[ii].subarray(0, start);
            ii++;
        }
    }

    if (reps.length > 0) {
        buffers.splice.apply(buffers, [ii, 0].concat(reps));
        ii += reps.length;
    }

    while (removed.length < howMany) {
        var buf = buffers[ii];
        var len = buf.length;
        var take = Math.min(len, howMany - removed.length);

        if (take === len) {
            removed.push(buf);
            buffers.splice(ii, 1);
        }
        else {
            removed.push(buf.subarray(0, take));
            buffers[ii] = buffers[ii].subarray(take);
        }
    }

    this.length -= removed.length;

    return removed;
};



Buffers.prototype.get = function get(i) {
    var pos = this.pos(i);

    return this.buffers[pos.buf][pos.offset];
};

Buffers.prototype.set = function set(i, b) {
    var pos = this.pos(i);

    return this.buffers[pos.buf].set(pos.offset, b);
};

Buffers.prototype.indexOf = function (needle, offset) {
    if ("string" === typeof needle) {
        needle = Buffer.from(needle);
    } else if (needle instanceof Buffer) {
        // already a buffer
    } else {
        throw new Error('Invalid type for a search string');
    }

    if (!needle.length) {
        return 0;
    }

    if (!this.length) {
        return -1;
    }

    var i = 0, j = 0, match = 0, mstart, pos = 0;

    // start search from a particular point in the virtual buffer
    if (offset) {
        var p = this.pos(offset);
        i = p.buf;
        j = p.offset;
        pos = offset;
    }

    // for each character in virtual buffer
    for (; ;) {
        while (j >= this.buffers[i].length) {
            j = 0;
            i++;

            if (i >= this.buffers.length) {
                // search string not found
                return -1;
            }
        }

        var char = this.buffers[i][j];

        if (char == needle[match]) {
            // keep track where match started
            if (match == 0) {
                mstart = {
                    i: i,
                    j: j,
                    pos: pos
                };
            }
            match++;
            if (match == needle.length) {
                // full match
                return mstart.pos;
            }
        } else if (match != 0) {
            // a partial match ended, go back to match starting position
            // this will continue the search at the next character
            i = mstart.i;
            j = mstart.j;
            pos = mstart.pos;
            match = 0;
        }

        j++;
        pos++;
    }
};

Buffers.prototype.toBuffer = function () {
    return this.subarray();
}

Buffers.prototype.toString = function (encoding, start, end) {
    return this.subarray(start, end).toString(encoding);
}


module.exports = Buffers;