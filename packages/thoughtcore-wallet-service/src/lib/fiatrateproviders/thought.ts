import { IProvider, IRates } from './provider.js';

export const Thought: IProvider = {
  name: 'Thought',
  getUrl(coin): string {
    return `https://thoughtnetwork.com/api/rates/${coin.toUpperCase()}?p=bws`;
  },
  parseFn(raw) {
    const rates: Array<IRates> = [];
    for (const d of raw) {
      if (d.code && d.rate) {
        rates.push({
          code: d.code,
          value: +d.rate
        });
      }
    }
    return rates;
  }
};
