import * as express from 'express';

import logger from '../../logger';
import { Config } from '../../services/config';
import { IEVMNetworkConfig } from '../../types/Config';


export async function Web3Proxy(req: express.Request, res: express.Response) {
  let { chain, network } = req.params;
  const chainConfig: IEVMNetworkConfig = Config.chainConfig({ chain, network });
  const provider = chainConfig.provider || (chainConfig.providers && chainConfig.providers![0]);

  if (provider && chainConfig.publicWeb3) {
    const { host, port } = provider;
    const url = `http://${host}:${port}`;
    let requestStream;
    let got = await import('got');
    if (req.body.jsonrpc) {
      const options = {
        uri: url,
        headers: { 'content-type': 'application/x-www-form-urlencoded' },
        method: req.method,
        body: JSON.stringify(req.body),
        json: true
      };
      requestStream = got.stream(url, options);
    } else {
      requestStream = req.pipe(got.stream(url) as any);
    }
    requestStream
      .on('error', (err: any) => {
        logger.error('Error streaming wallet utxos: %o', err.stack || err.message || err);
        res.status(500).send('An Error Has Occurred');
      })
      .pipe(res);
  } else {
    res.status(500).send(`This node is not configured with a web3 connection for ${chain} ${network} `);
  }
}
