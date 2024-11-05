import { IValidation } from '..';
const Thoughtcore = require('thoughtcore-lib');

export class ThtValidation implements IValidation {
  validateAddress(network: string, address: string): boolean {
    const Address = Thoughtcore.Address;
    // Regular Address: try Thought
    return Address.isValid(address, network);
  }

  validateUri(addressUri: string): boolean {
    // Check if the input is a valid uri or address
    const URI = Thoughtcore.URI;
    // Bip21 uri
    return URI.isValid(addressUri);
  }
}
