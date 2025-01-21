const winston = require('winston');
//import parseArgv from './utils/parseArgv';
//let args = parseArgv([], [{ arg: 'DEBUG', type: 'bool' }]);
//const logLevel = args.DEBUG ? 'debug' : (process.env.TCN_LOG_LEVEL || 'info');



const logger = winston.createLogger({
  transports: [
    new winston.transports.Console({
      level: 'debug',
    })
  ],
  format: winston.format.combine(
    winston.format.colorize(),
    winston.format.prettyPrint(),
    winston.format.splat(),
    winston.format.simple(),
    winston.format.printf(function (info) {
      // fallback in case the above formatters  don't work.
      // eg: logger.log({ some: 'object' })
      if (typeof info.message === 'object') {
        info.message = JSON.stringify(info.message, null, 4);
      }
      return `${info.level} :: ${new Date().toISOString()} :: ${info.message}`;
    })
  )
});


module.exports = logger;
