import { EthChain } from '../eth/index.ts';

export class MaticChain extends EthChain {
  constructor() {
    super();
    this.chain = 'MATIC'
  }
}
