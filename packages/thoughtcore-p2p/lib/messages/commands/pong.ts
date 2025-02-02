'use strict';

import thoughtcore from 'thoughtcore-lib';
import BufferReader from 'thoughtcore-lib';
import BufferWriter from 'thoughtcore-lib';
import BufferUtil from 'thoughtcore-lib';
import Hash from 'thoughtcore-lib';
import Preconditions from 'thoughtcore-lib'; // as $
import { Message } from '../message'; // as $
import { logger } from './../../logger';
import { utils } from '../utils';
import Deps from 'thoughtcore-lib';

/**
 * A message in response to a ping message.
 * @param {Number} arg - A nonce for the Pong message
 * @param {Object=} options
 * @extends Message
 * @constructor
 */
export class PongMessage extends Message {
  command: string;
  nonce: any;
  constructor(arg, options) {
    super(options);
    Message.call(this, options);
    this.command = 'pong';
    $.checkArgument(
      _.isUndefined(arg) || (BufferUtil.isBuffer(arg) && arg.length === 8),
      'First argument is expected to be an 8 byte buffer'
    );
    this.nonce = arg || utils.getNonce();
  }

  setPayload = function (payload) {
    var parser = new BufferReader(payload);
    this.nonce = parser.read(8);

    utils.checkFinished(parser);
  };

  getPayload = function () {
    return this.nonce;
  };

}
