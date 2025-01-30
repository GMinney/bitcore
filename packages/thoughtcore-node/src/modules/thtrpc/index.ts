import { EventEmitter } from 'events';
import { BaseModule } from '..';
import { IThtRpcNetworkConfig } from '../../types/Config';
import { ThoughtRpcStateProvider } from './api/csp';
import { ThoughtRpcEventAdapter } from './api/event-adapter';
import { ThtRpcRoutes } from './api/thtrpc-routes';
import { ThtRpcP2pWorker } from './rpc';
import { ThtRpcVerificationPeer } from './rpc/verification';

export default class THTRPCModule extends BaseModule {
  static startMonitor: EventEmitter;
  static endMonitor: EventEmitter;
  constructor(services: BaseModule['thoughtcoreServices'], network: string, _config: IThtRpcNetworkConfig) {
    super(services);
    services.CSP.registerService("THTRPC", new ThoughtRpcStateProvider());
    services.Api.app.use(ThtRpcRoutes);
    services.P2P.register("THTRPC", ThtRpcP2pWorker);
    services.Verification.register("THTRPC", ThtRpcVerificationPeer);

    if (!THTRPCModule.startMonitor) {
      const adapter = new ThoughtRpcEventAdapter(services, network);
      THTRPCModule.startMonitor = services.Event.events.on('start', async () => {
        await adapter.start();
      });
      THTRPCModule.endMonitor = services.Event.events.on('stop', async () => {
        await adapter.stop();
      });
    }
  }
}
