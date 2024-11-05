import { ObjectId, WithId } from 'mongodb';
import { MongoBound } from './base';
import os from 'os';
import { StorageService } from '../services/storage';
import { BaseModel } from './base';

export interface IState {
  _id?: ObjectId;
  initialSyncComplete: any;
  verifiedBlockHeight?: {
    [chain: string]: {
      [network: string]: number;
    };
  };
}

export class StateModel extends BaseModel<IState> {
  constructor(storage?: StorageService) {
    super('state', storage);
  }
  allowedPaging = [];

  onConnect() {}

  async getSingletonState(): Promise<WithId<MongoBound<IState>> | null> {
    return this.collection.findOneAndUpdate(
      {},
      { $setOnInsert: { created: new Date() } },
      { upsert: true, returnDocument: 'after' }
    );
  }

  async getSyncingNode(params: { chain: string; network: string }): Promise<string> {
    const { chain, network } = params;
    const state = await this.getSingletonState();
    if (!state){
      throw new Error("State null")
    }
    return state[`syncingNode:${chain}:${network}`];
  }

  async selfNominateSyncingNode(params: { chain: string; network: string; lastHeartBeat: any }) {
    const { chain, network, lastHeartBeat } = params;
    const singleState = await this.getSingletonState()!;
    if (!singleState){
      throw new Error("singlestate null")
    }
    return this.collection.findOneAndUpdate(
      {
        _id: singleState._id,
        $or: [
          { [`syncingNode:${chain}:${network}`]: { $exists: false } },
          { [`syncingNode:${chain}:${network}`]: lastHeartBeat }
        ]
      },
      { $set: { [`syncingNode:${chain}:${network}`]: `${os.hostname}:${process.pid}:${Date.now()}` } }
    );
  }

  async selfResignSyncingNode(params: { chain: string; network: string; lastHeartBeat: any }) {
    const { chain, network, lastHeartBeat } = params;
    const singleState = await this.getSingletonState();
    if (!singleState){
      throw new Error("singlestate null")
    }
    return this.collection.findOneAndUpdate(
      { _id: singleState._id, [`syncingNode:${chain}:${network}`]: lastHeartBeat },
      { $unset: { [`syncingNode:${chain}:${network}`]: true } }
    );
  }
}

export let StateStorage = new StateModel();
