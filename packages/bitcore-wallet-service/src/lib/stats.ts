import * as async from 'async';
import * as _ from 'lodash';
import moment from 'moment';
import * as mongodb from 'mongodb';
import config from '../config.ts';
import logger from './logger.ts';

const INITIAL_DATE = '2015-01-01';

export class Stats {
  network: string;
  coin: string;
  from: moment.MomentFormatSpecification;
  to: moment.MomentFormatSpecification;
  db: mongodb.Db;
  client: mongodb.MongoClient;

  constructor(opts) {
    opts = opts || {};

    this.network = opts.network || 'livenet';
    this.coin = opts.coin || 'btc';
    this.from = moment(opts.from || INITIAL_DATE).format('YYYY-MM-DD');
    this.to = moment(opts.to).format('YYYY-MM-DD');
  }

  run(cb) {
    let dbConfig = config.storageOpts.mongoDb;
    let uri = dbConfig.uri;

    // Always for stats!
    uri = uri + 'readPreference=secondaryPreferred';
    console.log('Connected to ', uri);

    if (!dbConfig.dbname) {
      return cb(new Error('No dbname at config.'));
    }

    let client = mongodb.MongoClient.connect(dbConfig.uri)
      .then(client => {
        this.db = client.db(dbConfig.dbname);
        this.client = client;

        this._getStats((err, stats) => {
          this.client.close()
            .then(stats => {
              return cb(null, stats);
            })
            .catch(err => {
              logger.error('%o', err)
            })
          return cb(err);
        });
      })
      .catch(err => { return cb(err); });
  }

  _getStats(cb) {
    let result = {};
    async.series(
      [
        next => {
          this._getNewWallets(next);
        },
        next => {
          this._getTxProposals(next);
        },
        next => {
          this._getFiatRates(next);
        }
      ],
      (err, results) => {
        if (err) return cb(err);
        result = { newWallets: results[0], txProposals: results[1], fiatRates: results[2] };
        return cb(null, result);
      }
    );
  }

  _getNewWallets(cb) { // Could potentially become a problem with _id.
    this.db
      .collection('stats_wallets')
      .find({
        '_id.network': this.network,
        '_id.coin': this.coin,
        '_id.day': {
          $gte: this.from,
          $lte: this.to
        }
      })
      .sort({
        '_id.day': 1
      })
      .toArray()
      .then( results => {
        const stats = {
          byDay: _.map(results, record => {
            const day = moment(record.day).format('YYYYMMDD');
            return {
              day,
              coin: record.coin,
              value: record.value,
              count: record.count ? record.count : record.value.count
            };
          })
        };
        return cb(null, stats);
      })
      .catch(err => {
        return cb(err);
      })
      
  }

  _getFiatRates(cb) {
    this.db
      .collection('stats_fiat_rates')
      .find({
        '_id.coin': this.coin,
        '_id.day': {
          $gte: this.from,
          $lte: this.to
        }
      })
      .sort({
        '_id.day': 1
      })
      .toArray()
      .then(results => {
        const stats = {
          byDay: _.map(results, record => {
            const day = moment(record.day).format('YYYYMMDD');
            return {
              day,
              coin: record.coin,
              value: record.value
            };
          })
        };
        return cb(null, stats);
      })
      .catch(err => {
        return cb(err);
      });


  }

  _getTxProposals(cb) {
    this.db
      .collection('stats_txps')
      .find({
        '_id.network': this.network,
        '_id.coin': this.coin,
        '_id.day': {
          $gte: this.from,
          $lte: this.to
        }
      })
      .sort({
        '_id.day': 1
      })
      .toArray()
      .then(results => {
        const stats = {
          nbByDay: [],
          amountByDay: []
        };
        _.each(results, record => {
          const day = moment(record.day).format('YYYYMMDD');
          stats.nbByDay.push({
            day,
            coin: record.coin,
            count: record.count ? record.count : record.value.count
          });
          stats.amountByDay.push({
            day,
            amount: record.amount ? record.amount : record.value.amount
          });
        });
        return cb(null, stats);
      })
      .catch(err => {
        return cb(err);
      })


  }
}
