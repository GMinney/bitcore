import { Transform } from 'stream';
import { CoinStorage } from '../../../models/coin';
import { IWallet } from '../../../models/wallet';
import { IWalletAddress, WalletAddressStorage } from '../../../models/walletAddress';
import { IThtRpcCoin, IThtRpcTransaction } from '../types';

export class ThoughtRpcDbWalletTransactions extends Transform {
  walletAddresses?: Array<IWalletAddress>;
  constructor(public wallet: IWallet) {
    super({ objectMode: true });
  }

  async getAddresses() {
    const { chain, network, _id } = this.wallet;
    if (!this.walletAddresses) {
      this.walletAddresses = await WalletAddressStorage.collection.find({ chain, network, wallet: _id }).toArray();
    }
    return this.walletAddresses;
  }

  getHistoryEntry(category: string, value: number, tx: IThtRpcTransaction, o?: IThtRpcCoin) {
    return (
      JSON.stringify({
        id: tx.txid,
        txid: tx.txid,
        fee: tx.fee,
        size: 0,
        category,
        satoshis: value,
        height: tx.blockHeight,
        blockTime: tx.blockTimeNormalized,
        ...(o && {
          address: o.address,
          outputIndex: o.mintIndex
        })
      }) + '\n'
    );
  }

  async _transform(tx: IThtRpcTransaction, _, done) {
    const { chain, network } = this.wallet;
    const outputs = await CoinStorage.collection
      .find(
        {
          chain,
          network,
          mintTxid: tx.txid
        },
        { batchSize: 10000 }
      )
      .project({ address: 1, wallets: 1, value: 1, mintIndex: 1 })
      .addCursorFlag('noCursorTimeout', true)
      .toArray();

    const walletAddressesObjs = await this.getAddresses();
    const walletAddresses = walletAddressesObjs.map(w => w.address);
    let sending = walletAddresses.includes(tx.from);
    for (const o of outputs) {
      const isSend = !walletAddresses.includes(o.address);
      const isMove = o.address != tx.from && walletAddresses.includes(o.address);
      if (sending) {
        const sendValue = -1 * Math.abs(o.value);
        if (isSend) {
          this.push(this.getHistoryEntry('send', sendValue, tx, o as IThtRpcCoin));
        } else if (isMove) {
          this.push(this.getHistoryEntry('move', sendValue, tx, o as IThtRpcCoin));
        }
      } else {
        const isReceiving = walletAddresses.includes(o.address);
        if (isReceiving) {
          const receiveValue = Math.abs(o.value);
          this.push(this.getHistoryEntry('receive', receiveValue, tx, o as IThtRpcCoin));
        }
      }
    }
    if (sending && tx.fee > 0) {
      this.push(this.getHistoryEntry('fee', -1 * Number(tx.fee), tx));
    }
    done();
  }
}
