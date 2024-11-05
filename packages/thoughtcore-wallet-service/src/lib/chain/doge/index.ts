import * as async from 'async';
import { ThoughtcoreLibDoge } from 'crypto-wallet-core';
import _ from 'lodash';
import { IChain } from '../index.ts';
import { Defaults } from "../../common/defaults.ts";
import { Constants } from '../../common/constants.ts';
import { Utils } from '../../common/utils.ts';
import { ClientError } from '../../errors/clienterror.ts';
import { Errors } from '../../errors/errordefinitions.ts';
import logger from '../../logger.ts';
import { TxProposal } from '../../model/index.ts';
import { ThtChain } from '../tht/index.ts';

export class DogeChain extends ThtChain implements IChain {
  constructor(private thoughtcoreLibDoge = ThoughtcoreLibDoge) {
    super(ThoughtcoreLibDoge);
  }

  selectTxInputs(server, txp, wallet, opts, cb) {
    const MAX_TX_SIZE_IN_KB = Defaults.MAX_TX_SIZE_IN_KB_DOGE;

    // todo: check inputs are ours and have enough value
    if (txp.inputs && !_.isEmpty(txp.inputs)) {
      if (!_.isNumber(txp.fee)) txp.fee = this.getEstimatedFee(txp, { conservativeEstimation: true });
      return cb(this.checkTx(txp));
    }

    const feeOpts = { conservativeEstimation: opts.payProUrl ? true : false };
    const txpAmount = txp.getTotalAmount();
    const baseTxpSize = this.getEstimatedSize(txp, feeOpts);
    const baseTxpFee = (baseTxpSize * txp.feePerKb) / 1000;
    const sizePerInput = this.getEstimatedSizeForSingleInput(txp, feeOpts);
    const feePerInput = (sizePerInput * txp.feePerKb) / 1000;

    logger.debug(
      `Amount ${Utils.formatAmountInTht(
        txpAmount
      )} baseSize ${baseTxpSize} baseTxpFee ${baseTxpFee} sizePerInput ${sizePerInput}  feePerInput ${feePerInput}`
    );

    const sanitizeUtxos = utxos => {
      const excludeIndex = _.reduce(
        opts.utxosToExclude,
        (res, val) => {
          res[val] = val;
          return res;
        },
        {}
      );

      return _.filter(utxos, utxo => {
        if (utxo.locked) return false;
        if (utxo.notions <= feePerInput) return false;
        if (txp.excludeUnconfirmedUtxos && !utxo.confirmations) return false;
        if (excludeIndex[utxo.txid + ':' + utxo.vout]) return false;
        return true;
      });
    };

    const select = (utxos, coin, cb) => {
      const totalValueInUtxos = _.sumBy(utxos, 'notions');
      const netValueInUtxos = totalValueInUtxos - (baseTxpFee - utxos.length * feePerInput);

      if (totalValueInUtxos < txpAmount) {
        logger.debug(
          'Total value in all utxos (' +
            Utils.formatAmountInTht(totalValueInUtxos) +
            ') is insufficient to cover for txp amount (' +
            Utils.formatAmountInTht(txpAmount) +
            ')'
        );
        return cb(Errors.INSUFFICIENT_FUNDS);
      }
      if (netValueInUtxos < txpAmount) {
        logger.debug(
          'Value after fees in all utxos (' +
            Utils.formatAmountInTht(netValueInUtxos) +
            ') is insufficient to cover for txp amount (' +
            Utils.formatAmountInTht(txpAmount) +
            ')'
        );

        return cb(
          new ClientError(
            Errors.codes.INSUFFICIENT_FUNDS_FOR_FEE,
            `${Errors.INSUFFICIENT_FUNDS_FOR_FEE.message}. RequiredFee: ${baseTxpFee} Coin: ${txp.coin} feePerKb: ${txp.feePerKb} Err2`,
            {
              coin: txp.coin,
              feePerKb: txp.feePerKb,
              requiredFee: baseTxpFee
            }
          )
        );
      }

      const bigInputThreshold = txpAmount * Defaults.UTXO_SELECTION_MAX_SINGLE_UTXO_FACTOR + (baseTxpFee + feePerInput);
      logger.debug('Big input threshold ' + Utils.formatAmountInTht(bigInputThreshold));

      const partitions = _.partition(utxos, utxo => {
        return utxo.notions > bigInputThreshold;
      });

      const bigInputs = _.sortBy(partitions[0], 'notions');
      const smallInputs = _.sortBy(partitions[1], utxo => {
        return -utxo.notions;
      });

      logger.debug('Considering ' + bigInputs.length + ' big inputs (' + Utils.formatUtxos(bigInputs) + ')');
      logger.debug('Considering ' + smallInputs.length + ' small inputs (' + Utils.formatUtxos(smallInputs) + ')');

      let total = 0;
      let netTotal = -baseTxpFee;
      let selected = [];
      let fee;
      let error;

      _.each(smallInputs, (input, i) => {
        logger.debug('Input #' + i + ': ' + Utils.formatUtxos(input));

        const netInputAmount = input.notions - feePerInput;

        logger.debug('The input contributes ' + Utils.formatAmountInTht(netInputAmount));

        selected.push(input);

        total += input.notions;
        netTotal += netInputAmount;

        const txpSize = baseTxpSize + selected.length * sizePerInput;
        fee = Math.round(baseTxpFee + selected.length * feePerInput);
        fee = Math.max(fee, this.thoughtcoreLibDoge.Transaction.DUST_AMOUNT);

        logger.debug('Tx size: ' + Utils.formatSize(txpSize) + ', Tx fee: ' + Utils.formatAmountInTht(fee));

        const feeVsAmountRatio = fee / txpAmount;
        const amountVsUtxoRatio = netInputAmount / txpAmount;

        // logger.debug('Fee/Tx amount: ' + Utils.formatRatio(feeVsAmountRatio) + ' (max: ' + Utils.formatRatio(Defaults.UTXO_SELECTION_MAX_FEE_VS_TX_AMOUNT_FACTOR) + ')');
        // logger.debug('Tx amount/Input amount:' + Utils.formatRatio(amountVsUtxoRatio) + ' (min: ' + Utils.formatRatio(Defaults.UTXO_SELECTION_MIN_TX_AMOUNT_VS_UTXO_FACTOR) + ')');

        if (txpSize / 1000 > MAX_TX_SIZE_IN_KB) {
          //          logger.debug('Breaking because tx size (' + Utils.formatSize(txpSize) + ') is too big (max: ' + Utils.formatSize(this.MAX_TX_SIZE_IN_KB * 1000.) + ')');
          error = Errors.TX_MAX_SIZE_EXCEEDED;
          return false;
        }

        if (!_.isEmpty(bigInputs)) {
          if (amountVsUtxoRatio < Defaults.UTXO_SELECTION_MIN_TX_AMOUNT_VS_UTXO_FACTOR) {
            // logger.debug('Breaking because utxo is too small compared to tx amount');
            return false;
          }

          if (feeVsAmountRatio > Defaults.UTXO_SELECTION_MAX_FEE_VS_TX_AMOUNT_FACTOR) {
            const feeVsSingleInputFeeRatio = fee / (baseTxpFee + feePerInput);
            // logger.debug('Fee/Single-input fee: ' + Utils.formatRatio(feeVsSingleInputFeeRatio) + ' (max: ' + Utils.formatRatio(Defaults.UTXO_SELECTION_MAX_FEE_VS_SINGLE_UTXO_FEE_FACTOR) + ')' + ' loses wrt single-input tx: ' + Utils.formatAmountInTht((selected.length - 1) * feePerInput));
            if (feeVsSingleInputFeeRatio > Defaults.UTXO_SELECTION_MAX_FEE_VS_SINGLE_UTXO_FEE_FACTOR) {
              // logger.debug('Breaking because fee is too significant compared to tx amount and it is too expensive compared to using single input');
              return false;
            }
          }
        }

        logger.debug(
          'Cumuled total so far: ' +
            Utils.formatAmountInTht(total) +
            ', Net total so far: ' +
            Utils.formatAmountInTht(netTotal)
        );

        if (netTotal >= txpAmount) {
          const changeAmount = Math.round(total - txpAmount - fee);
          logger.debug('Tx change: %o', Utils.formatAmountInTht(changeAmount));

          const dustThreshold = Math.max(Defaults.MIN_OUTPUT_AMOUNT, this.thoughtcoreLibDoge.Transaction.DUST_AMOUNT);
          if (changeAmount > 0 && changeAmount <= dustThreshold) {
            logger.debug(
              'Change below dust threshold (' +
                Utils.formatAmountInTht(dustThreshold) +
                '). Incrementing fee to remove change.'
            );
            // Remove dust change by incrementing fee
            fee += changeAmount;
          }

          return false;
        }
      });

      if (netTotal < txpAmount) {
        logger.debug(
          'Could not reach Txp total (' +
            Utils.formatAmountInTht(txpAmount) +
            '), still missing: ' +
            Utils.formatAmountInTht(txpAmount - netTotal)
        );

        selected = [];
        if (!_.isEmpty(bigInputs)) {
          const input = _.head(bigInputs);
          logger.debug('Using big input: %o', Utils.formatUtxos(input));
          total = input.notions;
          fee = Math.round(baseTxpFee + feePerInput);
          fee = Math.max(fee, this.thoughtcoreLibDoge.Transaction.DUST_AMOUNT);
          netTotal = total - fee;
          selected = [input];
        }
      }

      if (_.isEmpty(selected)) {
        // logger.debug('Could not find enough funds within this utxo subset');
        return cb(
          error ||
            new ClientError(
              Errors.codes.INSUFFICIENT_FUNDS_FOR_FEE,
              `${Errors.INSUFFICIENT_FUNDS_FOR_FEE.message}. RequiredFee: ${fee} Coin: ${txp.coin} feePerKb: ${txp.feePerKb} Err3`,
              {
                coin: txp.coin,
                feePerKb: txp.feePerKb,
                requiredFee: fee
              }
            )
        );
      }

      return cb(null, selected, fee);
    };

    // logger.debug('Selecting inputs for a ' + Utils.formatAmountInTht(txp.getTotalAmount()) + ' txp');

    server.getUtxosForCurrentWallet({}, (err, utxos) => {
      if (err) return cb(err);

      let totalAmount;
      let availableAmount;

      const balance = this.totalizeUtxos(utxos);
      if (txp.excludeUnconfirmedUtxos) {
        totalAmount = balance.totalConfirmedAmount;
        availableAmount = balance.availableConfirmedAmount;
      } else {
        totalAmount = balance.totalAmount;
        availableAmount = balance.availableAmount;
      }

      if (totalAmount < txp.getTotalAmount()) return cb(Errors.INSUFFICIENT_FUNDS);
      if (availableAmount < txp.getTotalAmount()) return cb(Errors.LOCKED_FUNDS);

      utxos = sanitizeUtxos(utxos);

      // logger.debug('Considering ' + utxos.length + ' utxos (' + Utils.formatUtxos(utxos) + ')');

      const groups = [6, 1];
      if (!txp.excludeUnconfirmedUtxos) groups.push(0);

      let inputs = [];
      let fee;
      let selectionError;
      let i = 0;
      let lastGroupLength;
      async.whilst(
        () => {
          return i < groups.length && _.isEmpty(inputs);
        },
        next => {
          const group = groups[i++];

          const candidateUtxos = _.filter(utxos, utxo => {
            return utxo.confirmations >= group;
          });

          // logger.debug('Group >= ' + group);

          // If this group does not have any new elements, skip it
          if (lastGroupLength === candidateUtxos.length) {
            // logger.debug('This group is identical to the one already explored');
            return next();
          }

          // logger.debug('Candidate utxos: ' + Utils.formatUtxos(candidateUtxos));

          lastGroupLength = candidateUtxos.length;

          select(candidateUtxos, txp.coin, (err, selectedInputs, selectedFee) => {
            if (err) {
              // logger.debug('No inputs selected on this group: ', err);
              selectionError = err;
              return next();
            }

            selectionError = null;
            inputs = selectedInputs;
            fee = selectedFee;

            logger.debug('Selected inputs from this group: ' + Utils.formatUtxos(inputs));
            logger.debug('Fee for this selection: ' + Utils.formatAmountInTht(fee));

            return next();
          });
        },
        err => {
          if (err) return cb(err);
          if (selectionError || _.isEmpty(inputs)) return cb(selectionError || new Error('Could not select tx inputs'));

          txp.setInputs(_.shuffle(inputs));
          txp.fee = fee;

          err = this.checkTx(txp);
          if (!err) {
            const change = _.sumBy(txp.inputs, 'notions') - _.sumBy(txp.outputs, 'amount') - txp.fee;
            logger.debug(
              'Successfully built transaction. Total fees: ' +
                Utils.formatAmountInTht(txp.fee) +
                ', total change: ' +
                Utils.formatAmountInTht(change)
            );
          } else {
            logger.warn('Error building transaction: %o', err);
          }

          return cb(err);
        }
      );
    });
  }

  getWalletSendMaxInfo(server, wallet, opts, cb) {
    server.getUtxosForCurrentWallet({}, (err, utxos) => {
      if (err) return cb(err);

      const MAX_TX_SIZE_IN_KB = Defaults.MAX_TX_SIZE_IN_KB_DOGE;

      const info = {
        size: 0,
        amount: 0,
        fee: 0,
        feePerKb: 0,
        inputs: [],
        utxosBelowFee: 0,
        amountBelowFee: 0,
        utxosAboveMaxSize: 0,
        amountAboveMaxSize: 0
      };

      let inputs = _.reject(utxos, 'locked');
      if (!!opts.excludeUnconfirmedUtxos) {
        inputs = _.filter(inputs, 'confirmations');
      }
      inputs = _.sortBy(inputs, input => {
        return -input.notions;
      });

      if (_.isEmpty(inputs)) return cb(null, info);

      server._getFeePerKb(wallet, opts, (err, feePerKb) => {
        if (err) return cb(err);

        info.feePerKb = feePerKb;

        const txp = TxProposal.create({
          walletId: server.walletId,
          coin: wallet.coin,
          addressType: wallet.addressType,
          network: wallet.network,
          walletM: wallet.m,
          walletN: wallet.n,
          feePerKb
        });

        const baseTxpSize = this.getEstimatedSize(txp, { conservativeEstimation: true });
        const sizePerInput = this.getEstimatedSizeForSingleInput(txp, { conservativeEstimation: true });
        const feePerInput = (sizePerInput * txp.feePerKb) / 1000;

        const partitionedByAmount = _.partition(inputs, input => {
          return input.notions > feePerInput;
        });

        info.utxosBelowFee = partitionedByAmount[1].length;
        info.amountBelowFee = _.sumBy(partitionedByAmount[1], 'notions');
        inputs = partitionedByAmount[0];

        _.each(inputs, (input, i) => {
          const sizeInKb = (baseTxpSize + (i + 1) * sizePerInput) / 1000;
          if (sizeInKb > MAX_TX_SIZE_IN_KB) {
            info.utxosAboveMaxSize = inputs.length - i;
            info.amountAboveMaxSize = _.sumBy(_.slice(inputs, i), 'notions');
            return false;
          }
          txp.inputs.push(input);
        });

        if (_.isEmpty(txp.inputs)) return cb(null, info);

        const fee = this.getEstimatedFee(txp, { conservativeEstimation: true });
        const amount = _.sumBy(txp.inputs, 'notions') - fee;
        info.size = this.getEstimatedSize(txp, { conservativeEstimation: true });
        info.fee = fee;
        info.amount = amount;
        if (amount < Defaults.MIN_OUTPUT_AMOUNT) return cb(null, info);

        if (opts.returnInputs) {
          info.inputs = _.shuffle(inputs);
        }

        return cb(null, info);
      });
    });
  }
}
