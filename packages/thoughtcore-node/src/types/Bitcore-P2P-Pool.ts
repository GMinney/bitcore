import { EventEmitter } from 'events';
export interface Peer {
  bestHeight: number;
}
export type ThoughtcoreP2pPool = EventEmitter & {
  connect: () => any;
  _connectedPeers: Peer[];
  sendMessage: (message: string) => any;
};
