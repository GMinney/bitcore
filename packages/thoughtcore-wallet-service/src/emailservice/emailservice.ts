#!/usr/bin/env node
import config from '../config.ts';
import { EmailService } from '../lib/emailservice.ts';
import logger from '../lib/logger.ts';

const emailService = new EmailService();
emailService.start(config, err => {
  if (err) throw err;

  logger.info('Email service started');
});
