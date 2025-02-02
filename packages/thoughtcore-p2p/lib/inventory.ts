'use strict';


import thoughtcore from 'thoughtcore-lib';
import BufferReader from 'thoughtcore-lib';
import BufferWriter from 'thoughtcore-lib';
import BufferUtil from 'thoughtcore-lib';
import Preconditions from 'thoughtcore-lib'; // as $
import Deps from 'thoughtcore-lib'; // as _
import { logger } from './logger';

/**
 * A constructor for inventory related Thought messages such as
 * "getdata", "inv" and "notfound".
 * @param {Object} obj
 * @param {Number} obj.type - Inventory.TYPE
 * @param {Buffer} obj.hash - The hash for the inventory
 * @constructor
 */
export class Inventory {
    type: number;
    hash: Buffer;

    // https://en.thought.it/wiki/Protocol_specification#Inventory_Vectors
    static TYPE = {
        ERROR: 0,
        TX: 1,
        BLOCK: 2,
        FILTERED_BLOCK: 3
    };
    static TYPE_NAME = [
        'ERROR',
        'TX',
        'BLOCK',
        'FILTERED_BLOCK'
    ];

    constructor(obj) {
        this.type = obj.type;
        if (!BufferUtil.isBuffer(obj.hash)) {
            throw new TypeError('Unexpected hash, expected to be a buffer');
        }
        this.hash = obj.hash;
    }

    /**
   * A convenience constructor for Inventory.
   * @param {Number} type - Inventory.TYPE
   * @param {Buffer|String} hash - The hash for the inventory
   * @returns {Inventory} - A new instance of Inventory
   */
    forItem = function (type, hash) {
        $.checkArgument(hash);
        if (_.isString(hash)) {
            hash = Buffer.from(hash, 'hex');
            hash = BufferUtil.reverse(hash);
        }
        return new Inventory({ type: type, hash: hash });
    };

    /**
     * A convenience constructor for Inventory for block inventory types.
     * @param {Buffer|String} hash - The hash for the block inventory
     * @returns {Inventory} - A new instance of Inventory
     */
    forBlock = function (hash) {
        return this.forItem(Inventory.TYPE.BLOCK, hash);
    };

    /**
     * A convenience constructor for Inventory for filtered/merkle block inventory types.
     * @param {Buffer|String} hash - The hash for the filtered block inventory
     * @returns {Inventory} - A new instance of Inventory
     */
    forFilteredBlock = function (hash) {
        return this.forItem(Inventory.TYPE.FILTERED_BLOCK, hash);
    };

    /**
     * A convenience constructor for Inventory for transaction inventory types.
     * @param {Buffer|String} hash - The hash for the transaction inventory
     * @returns {Inventory} - A new instance of Inventory
     */
    forTransaction = function (hash) {
        return this.forItem(Inventory.TYPE.TX, hash);
    };

    /**
     * 
     * @returns {Inventory} - Constructor from buffer
     * @param {Buffer} payload - Serialized buffer of the inventory
     */
    fromBuffer = function (payload) {
        var parser = new BufferReader(payload);
        var obj = {};
        obj.type = parser.readUInt32LE();
        obj.hash = parser.read(32);
        return new Inventory(obj);
    };

    /**
     * @returns {Inventory} - Constructor from bufferReader
     * @param {BufferWriter} br - An instance of BufferWriter
     */
    fromBufferReader = function (br) {
        var obj = {};
        obj.type = br.readUInt32LE();
        obj.hash = br.read(32);
        return new Inventory(obj);
    };

    /**
   * @returns {Buffer} - Serialized inventory
   */
    toBuffer = function () {
        var bw = new BufferWriter();
        bw.writeUInt32LE(this.type);
        bw.write(this.hash);
        return bw.concat();
    };

    /**
     * @param {BufferWriter} bw - An instance of BufferWriter
     */
    toBufferWriter = function (bw) {
        bw.writeUInt32LE(this.type);
        bw.write(this.hash);
        return bw;
    };

}





