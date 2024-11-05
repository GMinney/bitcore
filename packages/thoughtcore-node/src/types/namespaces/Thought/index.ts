import { ThoughtBlockType as BBT, BlockHeader, BlockHeaderObj } from './Block';
import {
  ThoughtAddress as BA,
  ThoughtInput,
  ThoughtInputObj,
  ThoughtOutput,
  ThoughtScript as BS,
  ThoughtTransactionType
} from './Transaction';

export type ThoughtBlockType = BBT;
export type ThoughtTransaction = ThoughtTransactionType;
export type ThoughtScript = BS;
export type ThoughtAddress = BA;

export type TransactionOutput = ThoughtOutput;
export type TransactionInput = ThoughtInput;
export type TransactionInputObj = ThoughtInputObj;

export type ThoughtHeader = BlockHeader;
export type ThoughtHeaderObj = BlockHeaderObj;
