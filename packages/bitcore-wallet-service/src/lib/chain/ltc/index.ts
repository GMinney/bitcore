import { BitcoreLibLtc } from 'crypto-wallet-core';
import _ from 'lodash';
import { IChain } from '../index.ts';
import { BtcChain } from '../btc/index.ts';

export class LtcChain extends BtcChain implements IChain {
  constructor() {
    super(BitcoreLibLtc);
  }
}
