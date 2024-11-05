const ThoughtcoreLib = require('thoughtcore-lib');
import { IDeriver } from '..';

export abstract class AbstractThoughtcoreLibDeriver implements IDeriver {
  public abstract thoughtcoreLib;

  deriveAddress(network, pubKey, addressIndex, isChange, addressType) {
    const changeNum = isChange ? 1 : 0;
    const path = `m/${changeNum}/${addressIndex}`;
    return this.deriveAddressWithPath(network, pubKey, path, addressType);
  }

  derivePrivateKey(network, xPriv, addressIndex, isChange, addressType) {
    const changeNum = isChange ? 1 : 0;
    const path = `m/${changeNum}/${addressIndex}`;
    return this.derivePrivateKeyWithPath(network, xPriv, path, addressType);
  }

  deriveAddressWithPath(network: string, xpubKey: string, path: string, addressType: string) {
    const xpub = new this.thoughtcoreLib.HDPublicKey(xpubKey, network);
    return this.getAddress(network, xpub.derive(path).publicKey, addressType);
  }

  derivePrivateKeyWithPath(network: string, xprivKey: string, path: string, addressType: string) {
    const xpriv = new this.thoughtcoreLib.HDPrivateKey(xprivKey, network);
    const privKey = xpriv.deriveChild(path).privateKey;
    const pubKey = privKey.publicKey;
    const address = this.getAddress(network, pubKey, addressType);
    return { address, privKey: privKey.toString(), pubKey: pubKey.toString(), path };
  }

  getAddress(network: string, pubKey, addressType: string) {
    pubKey = new this.thoughtcoreLib.PublicKey(pubKey);
    return new this.thoughtcoreLib.Address(pubKey, network, addressType).toString();
  }
}
export class ThtDeriver extends AbstractThoughtcoreLibDeriver {
  thoughtcoreLib = ThoughtcoreLib;
}
