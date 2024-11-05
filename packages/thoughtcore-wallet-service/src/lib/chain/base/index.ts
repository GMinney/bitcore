import { EthChain } from '../eth/index.ts';

export class BaseChain extends EthChain {
  constructor() {
    super();
    this.chain = 'BASE';
  }
}
