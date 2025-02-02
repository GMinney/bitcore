import Hash from "./hash.js";
import BufferWriter from "../encoding/bufferwriter.js";

/**
 * Creates a tag hash to ensure uniqueness of a message between purposes.
 * For example, if there's a potential for a collision of messages between
 *   multiple purposes, a tag can be added to guard against such collisions.
 * @link https://github.com/thought/bips/blob/master/bip-0340.mediawiki#Design (see 'Tagged Hashes')
 * @param {String} tag The tag to prevent message collisions. Should uniquely reflect the purpose of the message.
 * @param {Buffer|String} message (optional)
 * @param {String} messageEncoding (default: 'hex') If `message` is a string, provide the encoding
 * @returns {TaggedHash} Instance of a BufferWriter with the written tag and `finalize` method
 */
export default class TaggedHash extends BufferWriter {
  constructor(tag, message?, messageEncoding = 'hex') {
    super();
    if (!(this instanceof TaggedHash)) {
      return new TaggedHash(tag, message, messageEncoding);
    }
    BufferWriter.apply(this);
    tag = Buffer.from(tag);
  
    const taghash = Hash.sha256(tag);
    this.write(taghash);
    this.write(taghash);
    if (message) {
      message = Buffer.isBuffer(message) ? message : Buffer.from(message, messageEncoding);
      this.write(message);
    }
    Object.defineProperties(TaggedHash, {
      TAPSIGHASH: { get: () => new TaggedHash('TapSighash') },
      TAPLEAF: { get: () => new TaggedHash('TapLeaf') },
      TAPBRANCH: { get: () => new TaggedHash('TapBranch') }
    });
    return this;
  }

  /**
 * Returns a 32-byte SHA256 hash of the double tagged hashes concat'd with the message
 * as defined by BIP-340: SHA256(SHA256(tag), SHA256(tag), message)
 * @returns {Buffer}
 */
finalize = function () {
  return Buffer.from(Hash.sha256(this.toBuffer()));
};

};

