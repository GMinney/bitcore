#!/usr/bin/env node

const request = require('request');
const Thoughtcore = require('thoughtcore-lib');
import { Client } from '../lib/blockchainexplorers/v8/client.ts';

const coin = process.argv[2];

if (!coin) {
  console.log(' Usage: coin authKey (extra: tokenAddress= )');
  process.exit(1);
}

const network = 'mainnet';
const authKey = process.argv[3];
const path = process.argv[4] || 'addresses';
const extra = process.argv[5] || '';
// tokenAddress=$
console.log('COIN:', coin);

if (!authKey) throw new Error('provide authKey');

// ====================
//
const authKeyObj = Thoughtcore.PrivateKey(authKey);

let tmp = authKeyObj.toObject();
tmp.compressed = false;
const pubKey = Thoughtcore.PrivateKey(tmp).toPublicKey();

const BASE = {
  THT: `https://api.thoughtcore.io/api/${coin}/${network}`,
  BCH: `https://api.thoughtcore.io/api/${coin}/${network}`,
  ETH: `https://api-eth.thoughtcore.io/api/${coin}/${network}`,
  MATIC: `https://api-matic.thoughtcore.io/api/${coin}/${network}`,
  ARB: `https://api-eth.thoughtcore.io/api/ARB/${network}`,
  BASE: `https://api-eth.thoughtcore.io/api/BASE/${network}`,
  OP: `https://api-eth.thoughtcore.io/api/OP/${network}`,
  XRP: `https://api-xrp.thoughtcore.io/api/${coin}/${network}`,
  DOGE: `https://api.thoughtcore.io/api/${coin}/${network}`,
  LTC: `https://api.thoughtcore.io/api/${coin}/${network}`
};
let baseUrl = BASE[coin];
console.log('[v8tool.ts.37:baseUrl:]', baseUrl); // TODO

let client = new Client({
  baseUrl,
  authKey: authKeyObj
});

// utxos
// addresses

let url = `${baseUrl}/wallet/${pubKey}/${path}`;
if (extra) {
  url = url + '?' + extra;
}

console.log('[v8tool.ts.38:url:]', url); // TODO
console.log('[v8tool.37:url:]', url);
const signature = client.sign({ method: 'GET', url });
const payload = {};

request.get(
  url,
  {
    headers: { 'x-signature': signature },
    body: payload,
    json: true
  },
  (err, req, body) => {
    if (err) {
      console.log('[v8tool.43:err:]', err);
    } else {
      try {
        console.log('[v8tool.50:body:]', body);
      } catch (e) {
        console.log('[v8tool.52]', e, body);
      }
    }
  }
);
