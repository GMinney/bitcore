export interface ThoughtAddress {
  toString: (stripCash: boolean) => string;
}
export interface ThoughtScript {
  toBuffer: () => Buffer;
  toHex: () => string;
  classify: () => string;
  chunks: Array<{ buf: Buffer }>;
  toAddress: (network: string) => ThoughtAddress;
}
export interface ThoughtInputObj {
  prevTxId: string;
  outputIndex: number;
  sequenceNumber: number;
}
export interface ThoughtInput {
  toObject: () => ThoughtInputObj;
}
export interface ThoughtOutput {
  script: ThoughtScript;
  satoshis: number;
}
export interface ThoughtTransactionType {
  outputAmount: number;
  hash: string;
  _hash: undefined | string;
  isCoinbase: () => boolean;
  outputs: ThoughtOutput[];
  inputs: ThoughtInput[];
  toBuffer: () => Buffer;
  nLockTime: number;
}
