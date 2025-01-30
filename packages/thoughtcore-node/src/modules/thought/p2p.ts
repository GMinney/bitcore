import { EventEmitter } from 'events';
import logger, { timestamp } from '../../logger';
import { ThoughtBlock, ThoughtBlockStorage, IThtBlock } from '../../models/block';
import { StateStorage } from '../../models/state';
import { TransactionStorage } from '../../models/transaction';
import { ChainStateProvider } from '../../providers/chain-state';
import { Libs } from '../../providers/libs';
import { BaseP2PWorker } from '../../services/p2p';
import { SpentHeightIndicators } from '../../types/Coin';
import { wait } from '../../utils';
import { ThoughtBlockType, ThoughtHeaderObj, ThoughtTransaction } from '../../types/namespaces/Thought';

export class ThoughtP2PWorker extends BaseP2PWorker<IThtBlock> {
  protected thoughtcoreLib: any;
  protected thoughtcoreP2p: any;
  protected chainConfig: any;
  protected messages: any;
  protected connectInterval?: NodeJS.Timer;
  protected invCache: any;
  protected invCacheLimits: any;
  protected initialSyncComplete: boolean;
  protected blockModel: ThoughtBlock;
  protected pool: any;
  public events: EventEmitter;
  public isSyncing: boolean;
  constructor({ chain, network, chainConfig, blockModel = ThoughtBlockStorage }) {
    super({ chain, network, chainConfig, blockModel });
    this.blockModel = blockModel;
    this.chain = chain;
    this.network = network;
    this.thoughtcoreLib = Libs.get(chain).lib;
    this.thoughtcoreP2p = Libs.get(chain).p2p;
    this.chainConfig = chainConfig;
    this.events = new EventEmitter();
    this.isSyncing = false;
    this.initialSyncComplete = false;
    this.invCache = {};
    this.invCacheLimits = {
      [this.thoughtcoreP2p.Inventory.TYPE.BLOCK]: 100,
      [this.thoughtcoreP2p.Inventory.TYPE.TX]: 100000
    };
    this.messages = new this.thoughtcoreP2p.Messages({
      network: this.thoughtcoreLib.Networks.get(this.network)
    });
    this.pool = new this.thoughtcoreP2p.Pool({
      addrs: this.chainConfig.trustedPeers.map(peer => {
        return {
          ip: {
            v4: peer.host
          },
          port: peer.port
        };
      }),
      dnsSeed: false,
      listenAddr: false,
      network: this.network,
      messages: this.messages
    });
  }

  cacheInv(type: number, hash: string): void {
    if (!this.invCache[type]) {
      this.invCache[type] = [];
    }
    if (this.invCache[type].length > this.invCacheLimits[type]) {
      this.invCache[type].shift();
    }
    this.invCache[type].push(hash);
  }

  isCachedInv(type: number, hash: string): boolean {
    if (!this.invCache[type]) {
      this.invCache[type] = [];
    }
    return this.invCache[type].includes(hash);
  }

  setupListeners() {
    try {
      this.pool.on('peerready', peer => {
        logger.info(
          `${timestamp()} | Connected to peer: ${peer.host}:${peer.port.toString().padEnd(5)} | Chain: ${this.chain
          } | Network: ${this.network}`
        );
      });
  
      this.pool.on('peerconnect', peer => {
        logger.info(
          `${timestamp()} | Connected to peer: ${peer.host}:${peer.port.toString().padEnd(5)} | Chain: ${this.chain
          } | Network: ${this.network}`
        );
      });
  
      this.pool.on('peerdisconnect', peer => {
        logger.warn(
          `${timestamp()} | Not connected to peer: ${peer.host}:${peer.port.toString().padEnd(5)} | Chain: ${this.chain
          } | Network: ${this.network}`
        );
      });
  
      this.pool.on('peertx', async (peer, message) => {
        const hash = message.transaction.hash;
        logger.debug('peer tx received: %o', {
          peer: `${peer.host}:${peer.port}`,
          chain: this.chain,
          network: this.network,
          hash
        });
        if (this.isSyncingNode && !this.isCachedInv(this.thoughtcoreP2p.Inventory.TYPE.TX, hash)) {
          this.cacheInv(this.thoughtcoreP2p.Inventory.TYPE.TX, hash);
          await this.processTransaction(message.transaction);
          this.events.emit('transaction', message.transaction);
        }
      });
  
      this.pool.on('peerblock', async (peer, message) => {
        const { block } = message;
        const { hash } = block;
        logger.debug('peer block received: %o', {
          peer: `${peer.host}:${peer.port}`,
          chain: this.chain,
          network: this.network,
          hash
        });
  
        const blockInCache = this.isCachedInv(this.thoughtcoreP2p.Inventory.TYPE.BLOCK, hash);
        if (!blockInCache) {
          block.transactions.forEach(transaction => this.cacheInv(this.thoughtcoreP2p.Inventory.TYPE.TX, transaction.hash));
          this.cacheInv(this.thoughtcoreP2p.Inventory.TYPE.BLOCK, hash);
        }
        if (this.isSyncingNode && (!blockInCache || this.isSyncing)) {
          this.events.emit(hash, message.block);
          this.events.emit('block', message.block);
          if (!this.isSyncing) {
            this.sync();
          }
        }
      });
  
      this.pool.on('peerheaders', (peer, message) => {
        logger.debug('peerheaders message received: %o', {
          peer: `${peer.host}:${peer.port}`,
          chain: this.chain,
          network: this.network,
          count: message.headers.length
        });
        this.events.emit('headers', message.headers);
      });
  
      this.pool.on('peerinv', (peer, message) => {
        if (this.isSyncingNode) {
          const filtered = message.inventory.filter(inv => {
            const hash = this.thoughtcoreLib.encoding
              .BufferReader(inv.hash)
              .readReverse()
              .toString('hex');
            return !this.isCachedInv(inv.type, hash);
          });
  
          if (filtered.length) {
            peer.sendMessage(this.messages.GetData(filtered));
          }
        }
      });
    } catch (err) {
      logger.err(`An error has occurred while setting up listeners ${this.chain} ${this.network}: %o`, err);
    }
  }

  async connect() {
    try {
      logger.debug(`Attempting to connect ... Setting up listeners ...`);
      this.setupListeners();
      logger.debug(`Attempting to connect ... Connecting with pool ...`);
      this.pool.connect();
      logger.debug(`Attempting to connect ... Binding to pool ...`);
      this.connectInterval = setInterval(this.pool.connect.bind(this.pool), 5000);
      logger.debug(`Attempting to connect ... awaiting peerready to resolve ...`);
      // once is one time listener
      // See Nodejs.eventemitter#once
      return new Promise<void>(resolve => {
        this.pool.once('peerready', () => resolve());
      });
    } catch (err) {
      logger.err(`An error has occurred while attempting to connect() ${this.chain} ${this.network}: %o`, err);
    }
  }

  async disconnect() {
    try {
      this.pool.removeAllListeners();
      this.pool.disconnect();
      if (this.connectInterval) {
        clearInterval(this.connectInterval as NodeJS.Timeout);
      }
    } catch (err) {
      logger.err(`An error has occurred while attempting to disconnect() ${this.chain} ${this.network}: %o`, err);
    }
  }

  public async getHeaders(candidateHashes: string[]): Promise<ThoughtHeaderObj[]> {
    try {
      let received = false;
      return new Promise<ThoughtHeaderObj[]>(async resolve => {
        this.events.once('headers', headers => {
          received = true;
          resolve(headers);
        });
        while (!received) {
          this.pool.sendMessage(this.messages.GetHeaders({ starts: candidateHashes }));
          await wait(1000);
        }
      });
    } catch (err) {
      logger.err(`An error has occurred while attempting to getHeaders() ${this.chain} ${this.network}: %o`, err);
      return new Promise<ThoughtHeaderObj[]>(async resolve => {
        resolve([]);
      });
    }
  }

  public async getBlock(hash: string) {
    try {
      logger.debug(`Getting block, hash:`, hash);
      let received = false;
      return new Promise<ThoughtBlockType>(async resolve => {
        this.events.once(hash, (block: ThoughtBlockType) => {
          logger.debug(`Received block, hash: %o`, hash);
          received = true;
          resolve(block);
        });
        while (!received) {
          this.pool.sendMessage(this.messages.GetData.forBlock(hash));
          await wait(1000);
        }
      });
    } catch (err) {
      logger.err(`An error has occurred while attempting to getBlock() ${this.chain} ${this.network}: %o`, err);
      return new Promise<ThoughtBlockType>(async resolve => {
        resolve({} as ThoughtBlockType);
      });
    }
  }

  getBestPoolHeight(): number {
    try {
      let best = 0;
      for (const peer of Object.values(this.pool._connectedPeers) as { bestHeight: number }[]) {
        if (peer.bestHeight > best) {
          best = peer.bestHeight;
        }
      }
      return best;
    } catch (err) {
      logger.err(`An error has occurred while attempting to getBestPoolHeight() ${this.chain} ${this.network}: %o`, err);
      return 0;
    }
  }

  async processBlock(block: ThoughtBlockType): Promise<any> {
    try {
      await this.blockModel.addBlock({
        chain: this.chain,
        network: this.network,
        forkHeight: this.chainConfig.forkHeight,
        parentChain: this.chainConfig.parentChain,
        initialSyncComplete: this.initialSyncComplete,
        block
      });
    } catch (err) {
      logger.err(`An error has occurred while attempting to processBlock() ${this.chain} ${this.network}: %o`, err);
      return;
    }

  }

  async processTransaction(tx: ThoughtTransaction): Promise<any> {
    try {
      const now = new Date();
      await TransactionStorage.batchImport({
        chain: this.chain,
        network: this.network,
        txs: [tx],
        height: SpentHeightIndicators.pending,
        mempoolTime: now,
        blockTime: now,
        blockTimeNormalized: now,
        initialSyncComplete: true
      });
    } catch (err) {
      logger.err(`An error has occurred while attempting to processTransaction() ${this.chain} ${this.network}: %o`, err);
      return;
    }
  }

  async syncDone() {
    return new Promise(resolve => this.events.once('SYNCDONE', resolve));
  }

  async sync() {
    try {
      logger.info(`Attempting to sync ${this.chain} ${this.network}`);
      if (this.isSyncing) {
        return false;
      }
      this.isSyncing = true;
      const { chain, chainConfig, network } = this;
      const { parentChain, forkHeight } = chainConfig;
      const state = await StateStorage.collection.findOne({});
      this.initialSyncComplete =
        state && state.initialSyncComplete && state.initialSyncComplete.includes(`${chain}:${network}`);
      let tip = await ChainStateProvider.getLocalTip({ chain, network });
      if (parentChain && (!tip || tip.height < forkHeight)) {
        let parentTip = await ChainStateProvider.getLocalTip({ chain: parentChain, network });
        while (!parentTip || parentTip.height < forkHeight) {
          logger.info(`Waiting until ${parentChain} syncs before ${chain} ${network}`);
          await wait(5000);
          parentTip = await ChainStateProvider.getLocalTip({ chain: parentChain, network });
        }
      }
  
      const getHeaders = async () => {
        const locators = await ChainStateProvider.getLocatorHashes({ chain, network });
        return this.getHeaders(locators);
      };
  
      let headers = await getHeaders();
      while (headers.length > 0) {
        tip = await ChainStateProvider.getLocalTip({ chain, network });
        let currentHeight = tip ? tip.height : 0;
        const startingHeight = currentHeight;
        const startingTime = Date.now();
        let lastLog = startingTime;
        logger.info(`${timestamp()} | Syncing ${headers.length} blocks | Chain: ${chain} | Network: ${network}`);
        for (const header of headers) {
          try {
            const block = await this.getBlock(header.hash);
            await this.processBlock(block);
            currentHeight++;
            const now = Date.now();
            const oneSecond = 1000;
            if (now - lastLog > oneSecond) {
              const blocksProcessed = currentHeight - startingHeight;
              const elapsedMinutes = (now - startingTime) / (60 * oneSecond);
              logger.info(
                `${timestamp()} | Syncing... | Chain: ${chain} | Network: ${network} |${(blocksProcessed / elapsedMinutes)
                  .toFixed(2)
                  .padStart(8)} blocks/min | Height: ${currentHeight.toString().padStart(7)}`
              );
              lastLog = now;
            }
          } catch (err) {
            logger.error(`${timestamp()} | Error syncing | Chain: ${chain} | Network: ${network} | %o`, err);
            this.isSyncing = false;
            return this.sync();
          }
        }
        headers = await getHeaders();
      }
  
      logger.info(`${timestamp()} | Sync completed | Chain: ${chain} | Network: ${network}`);
      this.isSyncing = false;
      await StateStorage.collection.findOneAndUpdate(
        {},
        { $addToSet: { initialSyncComplete: `${chain}:${network}` } },
        { upsert: true }
      );
      this.events.emit('SYNCDONE');
      return true;
    } catch (err) {
      logger.err(`An error has occurred while syncing ${this.chain} ${this.network}: %o`, err);
    }
  }

  async stop() {
    try {
      this.stopping = true;
      logger.debug(`Stopping worker for chain ${this.chain}`);
      this.queuedRegistrations.forEach(timeoutId => clearTimeout(timeoutId as NodeJS.Timeout));
      await this.unregisterSyncingNode();
      await this.disconnect();
    } catch (err) {
      logger.err(`An error has occurred while stopping ${this.chain} ${this.network}: %o`, err);
    }
  }

  async start() {
    try {
      logger.debug(`Loaded module ./thought/p2p for chain ${this.chain}`);
      logger.debug(`Started worker for chain / Attempting Connections ${this.chain}`);
      await this.connect();
      logger.debug(`Promise to connect resolved, Refreshing syncing node for chain ${this.chain}`);
      this.refreshSyncingNode();
    } catch (err) {
      logger.err(`An error has occurred while starting ${this.chain} ${this.network}: %o`, err);
    }

  }
}
