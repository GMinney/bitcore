import { EthChain } from '../eth/index.ts';

export class OpChain extends EthChain {
  constructor() {
    super();
    this.chain = 'OP';
  }
}
