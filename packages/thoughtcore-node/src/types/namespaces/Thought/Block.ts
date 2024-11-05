import { ThoughtTransactionType } from './Transaction';
export interface BlockHeaderObj {
  prevHash: string;
  hash: string;
  time: number;
  version: number;
  merkleRoot: string;
  bits: number;
  nonce: number;
}
export interface BlockHeader {
  toObject: () => BlockHeaderObj;
}
export interface ThoughtBlockType {
  hash: string;
  transactions: ThoughtTransactionType[];
  header: BlockHeader;
  toBuffer: () => Buffer;
}
