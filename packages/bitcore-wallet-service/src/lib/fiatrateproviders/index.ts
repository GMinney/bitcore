import { BitPay } from './bitpay.ts';
import { IProvider } from './provider.ts';
// import { Bitstamp } from './bitstamp';


export const providers: IProvider[] = [
  BitPay // the first in the array is the default rate source
];
