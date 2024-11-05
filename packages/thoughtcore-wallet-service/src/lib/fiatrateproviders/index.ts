import { Thought } from './thoughtnetwork.ts';
import { IProvider } from './provider.js';
// import { Bitstamp } from './bitstamp';


export const providers: IProvider[] = [
  Thought // the first in the array is the default rate source
];
