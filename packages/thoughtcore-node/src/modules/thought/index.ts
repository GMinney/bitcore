import { BaseModule } from '..';
import { THTStateProvider } from '../../providers/chain-state/thought/tht';
import { ThoughtP2PWorker } from './p2p';
import { VerificationPeer } from './VerificationPeer';

export default class ThoughtModule extends BaseModule {
  constructor(services: BaseModule['thoughtcoreServices']) {
    super(services);
    services.Libs.register('THT', 'thoughtcore-lib', 'thoughtcore-p2p');
    services.P2P.register('THT', ThoughtP2PWorker);
    services.CSP.registerService('THT', new THTStateProvider());
    services.Verification.register('THT', VerificationPeer);
  }
}
