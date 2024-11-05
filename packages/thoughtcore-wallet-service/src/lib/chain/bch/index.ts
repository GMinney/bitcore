import { ThoughtcoreLibCash } from 'crypto-wallet-core';
import { IChain } from '../index.ts';
import config from '../../../config.ts';
import { Errors } from '../../errors/errordefinitions.ts';
import { ThtChain } from '../tht/index.ts';

export class BchChain extends ThtChain implements IChain {
  constructor() {
    super(ThoughtcoreLibCash);
    this.sizeEstimationMargin = config.bch?.sizeEstimationMargin ?? 0.01;
    this.inputSizeEstimationMargin = config.bch?.inputSizeEstimationMargin ?? 2;
  }
  getSizeSafetyMargin(opts: any): number {
    return 0;
  }

  getInputSizeSafetyMargin(opts: any): number {
    return 0;
  }

  validateAddress(wallet, inaddr, opts) {
    const A = ThoughtcoreLibCash.Address;
    let addr: {
      network?: string;
      toString?: (cashAddr: boolean) => string;
    } = {};
    try {
      addr = new A(inaddr);
    } catch (ex) {
      throw Errors.INVALID_ADDRESS;
    }
    if (!this._isCorrectNetwork(wallet, addr)) {
      throw Errors.INCORRECT_ADDRESS_NETWORK;
    }
    if (!opts.noCashAddr) {
      if (addr.toString(true) != inaddr) throw Errors.ONLY_CASHADDR;
    }
    return;
  }
}
