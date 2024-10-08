//import requestStream from 'got';
//import got from 'got-promise-native';
import got, { GotJSONOptions, GotOptions } from 'got';
import * as http from 'http'
import * as secp256k1 from 'secp256k1';
import { URL } from 'url';
import * as utils from './utils';
let usingBrowser = (global as any).window;
const URLClass = usingBrowser ? usingBrowser.URL : URL;
const bitcoreLib = require('crypto-wallet-core').BitcoreLib;


export class Client {
  apiUrl: string;
  authKey: any; s
  constructor(params) {
    Object.assign(this, params);
  }


  async _request(url: string, params: GotJSONOptions) {
    try {
      return await got(url, params);
    } catch (err) {
      if (err.statusCode) {
        throw new Error(`${err.statusCode} - ${params.method}: ${url} - "${JSON.stringify(err.error)}"`);
      } else if (err.error instanceof Error) {
        throw err.error;
      }
      throw err;
    }
  }

  _buildQueryString(params: any) {
    let query = [];
    for (const [key, value] of Object.entries(params)) {
      value && query.push(`${key}=${value}`);
    }
    return query.length ? `?${query.join('&')}` : '';
  }

  getMessage(params: { method: string; url: string; payload?: any }) {
    const { method, url, payload = {} } = params;
    const parsedUrl = new URLClass(url);
    return [method, parsedUrl.pathname + parsedUrl.search, JSON.stringify(payload)].join('|');
  }

  sign(params: { method: string; url: string; payload?: any }) {
    const message = this.getMessage(params);
    const privateKey = this.authKey.toBuffer();
    const messageHash = bitcoreLib.crypto.Hash.sha256sha256(Buffer.from(message));
    return secp256k1.ecdsaSign(messageHash, privateKey).signature.toString();
  }

  async register(params: { payload: { baseUrl?: string } & any }) {
    const { payload } = params;
    // allow you to overload the client's baseUrl
    const { baseUrl = this.apiUrl } = payload;
    const url = `${baseUrl}/wallet`;
    const signature = this.sign({ method: 'POST', url, payload });
    return this._request(url, {
      method: 'POST',
      baseUrl: baseUrl,
      headers: { 'x-signature': signature },
      body: payload,
      json: true
    });
  }

  async getToken(contractAddress) {
    const url = `${this.apiUrl}/token/${contractAddress}`;
    return this._request(url, { method: 'GET', json: true });
  }

  async getBalance(params: { payload?: any; pubKey: string; time?: string }) {
    const { payload, pubKey, time } = params;
    let url = `${this.apiUrl}/wallet/${pubKey}/balance`;
    if (time) {
      url += `/${time}`;
    }
    if (payload && payload.tokenContractAddress) {
      url += `?tokenAddress=${payload.tokenContractAddress}`;
    }
    const signature = this.sign({ method: 'GET', url });
    return this._request(url, {
      method: 'GET',
      headers: { 'x-signature': signature },
      json: true
    });
  }

  async getTransaction(params: { txid: string, populated?: boolean }) {
    const { txid, populated } = params;
    let url = `${this.apiUrl}/tx/${txid}${populated ? '/populated' : ''}`;
    return this._request(url, { method: 'GET', json: true });
  }

  async getNonce(params) {
    const { address } = params;
    const url = `${this.apiUrl}/address/${address}/txs/count`;
    return this._request(url, { method: 'GET', json: true });
  }

  getAddressTxos = async function (params) {
    const { unspent, address } = params;
    const args = unspent ? `?unspent=${unspent}` : '';
    const url = `${this.apiUrl}/address/${address}${args}`;
    return this._request(url, { method: 'GET', json: true });
  };

  getCoins(params: { payload?: any; pubKey: string; includeSpent: boolean }) {
    const { payload, pubKey, includeSpent } = params;
    const url = `${this.apiUrl}/wallet/${pubKey}/utxos?includeSpent=${includeSpent}`;
    const signature = this.sign({ method: 'GET', url, payload });
    const streamParams: GotJSONOptions = {
      headers: { 'x-signature': signature },
      body: payload,
      json: true
    }
    return got.stream.get(url, streamParams);
  }

  listTransactions(params) {
    const { pubKey, startBlock, startDate, endBlock, endDate, includeMempool, payload, tokenContractAddress } = params;
    let url = `${this.apiUrl}/wallet/${pubKey}/transactions`;
    let query = '';
    if (startBlock) {
      query += `startBlock=${startBlock}&`;
    }
    if (endBlock) {
      query += `endBlock=${endBlock}&`;
    }
    if (startDate) {
      query += `startDate=${startDate}&`;
    }
    if (endDate) {
      query += `endDate=${endDate}&`;
    }
    if (includeMempool) {
      query += 'includeMempool=true';
    }
    if (tokenContractAddress) {
      query += `tokenAddress=${tokenContractAddress}`;
    }
    if (query) {
      url += '?' + query;
    }
    const signature = this.sign({ method: 'GET', url, payload });
    const streamParams: GotJSONOptions = {
      headers: { 'x-signature': signature },
      body: payload,
      json: true
    }
    return got.stream.get(url, streamParams);
  }

  async getFee(params) {
    const { target, txType } = params;
    const url = `${this.apiUrl}/fee/${target}${this._buildQueryString({ txType })}`;
    const result = await this._request(url, { method: 'GET', json: true });
    const responseBody = result.body;
    if (responseBody.errors?.length) {
      throw new Error(responseBody.errors[0]);
    }
    return result;
  }

  async getPriorityFee(params) {
    const { percentile } = params;
    let url = `${this.apiUrl}/priorityFee/${percentile}`;
    const result = await this._request(url, { method: 'GET', json: true });
    const responseBody = result.body;
    if (responseBody.errors?.length) {
      throw new Error(responseBody.errors[0]);
    }
    return result;
  }

  async importAddresses(params: { pubKey: string; payload: Array<{ address: string }> }): Promise<void> {
    const { payload, pubKey } = params;
    const url = `${this.apiUrl}/wallet/${pubKey}`;

    for (let i = 0; i < payload.length; i += 100) {
      const body = payload.slice(i, i + 100);
      console.log(`Importing addresses ${i + 1}-${i + body.length} of ${payload.length}`);
      if (i >= 100) {
        await utils.sleep(1000); // cooldown
      }

      const signature = this.sign({ method: 'POST', url, payload: body });

      await this._request(url, {
        method: 'POST',
        headers: { 'x-signature': signature },
        body,
        json: true
      });
    }
  }

  async broadcast(params) {
    const { payload } = params;
    const url = `${this.apiUrl}/tx/send`;
    return this._request(url, { method: 'POST', body: payload, json: true });
  }

  async checkWallet(params) {
    const { pubKey } = params;
    const url = `${this.apiUrl}/wallet/${pubKey}/check`;
    const signature = this.sign({ method: 'GET', url });
    return this._request(url, {
      method: 'GET',
      headers: { 'x-signature': signature },
      json: true
    });
  }

  getAddresses(params: { pubKey: string }) {
    const { pubKey } = params;
    const url = `${this.apiUrl}/wallet/${pubKey}/addresses`;
    const signature = this.sign({ method: 'GET', url });
    return this._request(url, {
      method: 'GET',
      headers: { 'x-signature': signature },
      json: true
    });
  }

  getAccountFlags(params: { address: string }) {
    const { address } = params;
    const url = `${this.apiUrl}/address/${address}/flags`;
    return this._request(url, { method: 'GET', json: true });
  }
}
