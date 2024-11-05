import { ThoughtTransactionType } from './Transaction';
export interface BlockHeaderObj {
  hash: string;
  confirmations: number;
  height: number;
  version: number;
  versionHex: string;
  merkleroot: string;
  time: number;
  mediantime: number;
  nonce: number;
  bits: number;
  difficulty: number;
  chainwork: string;
  cuckooProof: number[];
  previousblockhash: string;
  nextblockhash: string;

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
