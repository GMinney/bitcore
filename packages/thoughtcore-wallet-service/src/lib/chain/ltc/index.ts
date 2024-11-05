import { ThoughtcoreLibLtc } from 'crypto-wallet-core';
import _ from 'lodash';
import { IChain } from '../index.ts';
import { ThtChain } from '../tht/index.ts';

export class LtcChain extends ThtChain implements IChain {
  constructor() {
    super(ThoughtcoreLibLtc);
  }
}
