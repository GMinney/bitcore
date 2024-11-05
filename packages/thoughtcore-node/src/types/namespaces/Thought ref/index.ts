import { ThoughtBlockType as TBT, BlockHeader, BlockHeaderObj } from './Block';
import {
  ThoughtAddress as TA,
  ThoughtInput,
  ThoughtInputObj,
  ThoughtOutput,
  ThoughtScript as TS,
  ThoughtTransactionType
} from './Transaction';

export type ThoughtBlockType = TBT;
export type ThoughtTransaction = ThoughtTransactionType;
export type ThoughtScript = TS;
export type ThoughtAddress = TA;

export type TransactionOutput = ThoughtOutput;
export type TransactionInput = ThoughtInput;
export type TransactionInputObj = ThoughtInputObj;

export type ThoughtHeader = BlockHeader;
export type ThoughtHeaderObj = BlockHeaderObj;
