import { CryptoRpc } from 'crypto-rpc';
import { ObjectId } from 'mongodb';
import { BaseModule } from '../..';
import logger from '../../../logger';
import { ThoughtRpcStateProvider } from './csp';

export class ThoughtRpcEventAdapter {
  stopping = false;
  clients: CryptoRpc[] = [];
  constructor(protected services: BaseModule['thoughtcoreServices'], protected network: string) {}

  async start() {
    this.stopping = false;
    console.log('Starting websocket adapter for ThoughtRpc');

    this.services.Event.events.on('stop', () => {
      this.stop();
    });

    this.services.Event.events.on('start', async () => {
      const networks = this.services.Config.chainNetworks()
        .filter(c => c.chain === 'THTRPC')
        .map(c => c.network);
      const chain = 'THTRPC';
      const csp = this.services.CSP.get({ chain }) as ThoughtRpcStateProvider;

      for (let network of networks) {
        try {
          const client = await csp.getClient(network);

          client.rpc.on('ledgerClosed', async ledger => {
            if (this.stopping) {
              return;
            }
            this.services.Event.blockEvent.emit('block', { chain, network, ...ledger });
          });

          client.rpc.on('disconnected', () => {
            if (!this.stopping) {
              client.rpc.connect();
            }
          });

          client.rpc.on('transaction', async tx => {
            if (this.stopping) {
              return;
            }
            const address = tx.transaction.Account;
            const transformedTx = { ...csp.transform(tx, network), wallets: new Array<ObjectId>() };
            if ('chain' in transformedTx) {
              const transformedCoins = csp.transformToCoins(tx, network);
              const { transaction, coins } = await csp.tag(chain, network, transformedTx, transformedCoins);
              this.services.Event.txEvent.emit('tx', { ...transaction });
              if (coins && coins.length) {
                for (const coin of coins) {
                  this.services.Event.addressCoinEvent.emit('coin', { address, coin });
                }
              }
            }
          });

          await client.asyncRequest('subscribe', { streams: ['ledger', 'transactions_proposed'] });
        } catch (e: any) {
          logger.error('Error connecting to THTRPC: %o', e);
        }
      }
    });
  }

  async stop() {
    this.stopping = true;
    for (const client of this.clients) {
      client.rpc.removeAllListeners();
      await client.asyncRequest('unsubscribe', { streams: ['ledger', 'transactions_proposed'] });
      client.rpc.disconnect();
    }
  }
}
