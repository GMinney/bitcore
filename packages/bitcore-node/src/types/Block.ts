export interface IBlock {
  chain: string;
  network: string;

  hash: string;
  confirmations?: number;
  size: number;
  height: number;

  time: Date;
  timeNormalized: Date;

  nextBlockHash: string;
  transactions?: string[];
  transactionCount: number;

  reward: number;
  processed: boolean;

  previousBlockHash: string;
}
