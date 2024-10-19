import * as async from 'async';
import * as _ from 'lodash';
import moment from 'moment';
import * as mongodb from 'mongodb';
import {Document, WithId} from 'mongodb';
import config from '../config.ts';
import logger from './logger.ts';
import { Storage } from './storage.ts';

const ObjectID = mongodb.ObjectId;

var objectIdDate = function(date) {
  return Math.floor(date.getTime() / 1000).toString(16) + '0000000000000000';
};



export class CleanFiatRates {
  db: mongodb.Db;
  client: mongodb.MongoClient;
  from: Date;
  to: Date;

  constructor() {}

  run(cb) {
    let dbConfig = config.storageOpts.mongoDb;

    let uri = dbConfig.uri;

    uri = uri + 'readPreference=secondaryPreferred';
    console.log('Connected to ', uri);

    if (!dbConfig.dbname) {
      return cb(new Error('No dbname at config.'));
    }

    try {
      mongodb.MongoClient.connect(dbConfig.uri)
      .then(client => {
        this.db = client.db(dbConfig.dbname);
        this.client = client;
        this.cleanFiatRates(cb);
        this.client.close().catch(err => {});
      })
      .catch(err => {
        logger.error('%o', err);
        return cb(null);
      });
    } catch (err) {
      return cb(err);
    }
      
  }

  cleanFiatRates(cb) {
    let dates;
    async.series(
      [
        next => {
          console.log('## Getting dates to keep...');
          this._getDatesToKeep((err, res) => {
            if (err) {
              return next(err);
            }
            dates = res;
            next();
          });
        },
        next => {
          console.log('## Cleaning fiat rates...');
          this._cleanFiatRates(dates, next);
        }
      ],
      (err, results) => {
        if (err) return cb(err);
        return cb(null, results[1]);
      }
    );
  }



  async _getDatesToKeep(cb) {
    this.from = new Date();
    this.from.setMonth(this.from.getMonth() - 2); // from 2 month ago
    console.log(`\tFrom: ${moment(this.from).toDate()}`);

    this.to = new Date();
    this.to.setMonth(this.to.getMonth() - 1); // to 1 month ago
    console.log(`\tTo: ${moment(this.to).toDate()}`);

    const objectIdFromDate = objectIdDate(this.from);
    const objectIdToDate = objectIdDate(this.to);

    this.db
      .collection(Storage.collections.FIAT_RATES2)
      .find({
        _id: {
          $gte: new ObjectID(objectIdFromDate),
          $lte: new ObjectID(objectIdToDate)
        }
      })
      .sort({ _id: 1 })
      .toArray()
      .then( results => {

        const datesToKeep = [];

        // Timestamps grouped by coin avoiding duplicates.
        let tsGruopedByCoin = _.reduce(
          results,
          (r, a) => {
            r[a.coin] = _.uniq([...(r[a.coin] || []), a.ts]);
            return r;
          },
          {}
        );

        // keep one date every hour for each coin
        _.forEach(tsGruopedByCoin, (tsGroup, key) => {
          console.log(`\tFiltering times for ${key.toUpperCase()}`);
          let prevTime = null;
          let isSameHour;

          _.forEach(tsGroup, (ts: number) => {
            if (prevTime > ts + 60 * 10000) return cb(new Error('Results not in order'));

            if (prevTime) {
              isSameHour = moment(prevTime).isSame(moment(ts), 'hour');
              if (!isSameHour) {
                datesToKeep.push(ts);
              }
            } else {
              // keep first date in case prevTime doesn't exist.
              datesToKeep.push(ts);
            }
            prevTime = ts;
          });
        });
        return cb(null, datesToKeep);
      })
      .catch(err => {
        return cb(err);
      });
  }

  async _cleanFiatRates(datesToKeep, cb) {
    try {
      this.db
        .collection(Storage.collections.FIAT_RATES2)
        .deleteMany({
          ts: {
            $nin: datesToKeep,
            $gte: moment(this.from).valueOf(),
            $lte: moment(this.to).valueOf()
          }
        })
        .then(data => {
          console.log(`\t${data.deletedCount} entries were removed from fiat_rates2`);
          return cb(null, data.acknowledged);
        });
    } catch (err) {
      console.log('\t!! Cannot remove data from fiat_rates2:', err);
      return cb(err);
    }
  }
}
