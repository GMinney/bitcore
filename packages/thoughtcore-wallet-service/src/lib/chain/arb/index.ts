import { EthChain } from '../eth/index.ts';

export class ArbChain extends EthChain {
  constructor() {
    super();
    this.chain = 'ARB';
  }
}
