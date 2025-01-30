import { AccountTxResponse, Transaction, TransactionMetadata } from 'xrpl';
import { ITransaction } from '../../models/baseTransaction';
import { ICoin } from '../../models/coin';
import { IBlock } from '../../types/Block';

export type IThtRpcBlock = IBlock & {};
export type IThtRpcTransaction = ITransaction & {
  from: string;
  to?: string;
  nonce: number;
  currency?: string;
  invoiceID?: string;
};

export interface ThtRpcTransactionJSON {
  txid: string;
  chain: string;
  network: string;
  blockHeight: number;
  blockHash: string;
  blockTime: string;
  blockTimeNormalized: string;
  fee: bigint | number;
  value: number;
  from: string;
  to: string;
  nonce: number;
  currency?: string;
  invoiceID?: string;
}

export type IThtRpcCoin = ICoin & {};

export type AccountTransaction = AccountTxResponse['result']['transactions'][0]

export type BlockTransaction = Transaction & { hash: string, metaData?: TransactionMetadata };

export type RpcTransaction = Transaction & {
  DeliverMax: string,
  ctid: string,
  date: number,
  hash: string,
  inLedger: number,
  ledger_index: number,
  meta: TransactionMetadata,
  validated: boolean
};
