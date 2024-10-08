#!/usr/bin/env node
import _ from 'lodash';
import config from '../config.ts';
import { BlockchainMonitor } from '../lib/blockchainmonitor.ts';
import logger from '../lib/logger.ts';

const bcm = new BlockchainMonitor();
bcm.start(config, err => {
  if (err) throw err;

  logger.info('Blockchain monitor started');
});
