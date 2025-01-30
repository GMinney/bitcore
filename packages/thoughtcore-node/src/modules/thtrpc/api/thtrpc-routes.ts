import { Router } from 'express';
import logger from '../../../logger';
import { THTRPC } from './csp';
export const ThtRpcRoutes = Router();

ThtRpcRoutes.get('/api/THTRPC/:network/address/:address/txs/count', async (req, res) => {
  let { network, address } = req.params;
  try {
    const nonce = await THTRPC.getAccountNonce(network, address);
    res.json({ nonce });
  } catch (err: any) {
    logger.error('Error getting THTRPC account nonce: %o', err.stack || err.message || err);
    res.status(500).send(err.message || err);
  }
});

ThtRpcRoutes.get('/api/THTRPC/:network/address/:address/flags', async (req, res) => {
  let { address, network } = req.params;
  try {
    const flags = await THTRPC.getAccountFlags(network, address);
    res.json({ flags });
  } catch (err: any) {
    logger.error('Error getting THTRPC account flags: %o', err.stack || err.message || err);
    res.status(500).send(err.message || err);
  }
});

ThtRpcRoutes.get('/api/THTRPC/:network/reserve', async (req, res) => {
  let { network } = req.params;
  try {
    const reserve = await THTRPC.getReserve(network);
    res.json({ reserve });
  } catch (err: any) {
    logger.error('Error getting THTRPC reserve: %o', err.stack || err.message || err);
    res.status(500).send(err.message || err);
  }
});
