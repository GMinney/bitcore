#!/usr/bin/env node
import config from '../config.ts';
import logger from '../lib/logger.ts';
import { PushNotificationsService } from '../lib/pushnotificationsservice.ts';

const pushNotificationsService = new PushNotificationsService();
pushNotificationsService.start(config, err => {
  if (err) throw err;

  logger.info('Push Notification Service started');
});
