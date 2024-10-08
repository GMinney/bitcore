#!/usr/bin/env node

import config from '../config.ts';
import { FiatRateService } from '../lib/fiatrateservice.ts';
import logger from '../lib/logger.ts';

const service = new FiatRateService();
service.init(config, err => {
  if (err) throw err;
  service.startCron(config, err => {
    if (err) throw err;

    logger.info('Fiat rate service started');
  });
});
