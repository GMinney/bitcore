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
 * A message in response to a version message.
 * @extends Message
 * @constructor
 */
export class VerackMessage extends Message {
  constructor(arg, options) {
    super(options);
    if (!arg) {
      arg = {};
    }
    Message.call(this, options);
    this.command = 'verack';
  }


  setPayload = function (payload) { };

  getPayload = function () {
    return BufferUtil.EMPTY_BUFFER;
  };

}

// For a verack message, this is the structure:
//
//  Header:  F9BEB4D976657261636B000000000000000000005DF6E0E2
//  (No payload)
//
// Verack Message:
// ┌─────────────┬──────────────┬───────────────┬───────┬─────────────────────────────────────┐
// │ Name        │ Example Data │ Format        │ Size  │ Example Bytes                       │
// ├─────────────┼──────────────┼───────────────┼───────┼─────────────────────────────────────┤
// │ Magic Bytes │              │ bytes         │     4 │ F9 BE B4 D9                         │
// │ Command     │ "verack"     │ ascii bytes   │    12 │ 76 65 72 61 63 6B 00 00 00 00 00 00 │
// │ Size        │ 0            │ little-endian │     0 │ 00 00 00 00                         │
// │ Checksum    │              │ bytes         │     4 │ 5D F6 E0 E2                         │
// └─────────────┴──────────────┴───────────────┴───────┴─────────────────────────────────────┘



