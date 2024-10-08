import { ethers } from 'ethers';
import Web3 from 'web3';
import { AbiItem } from 'web3-utils';
import { Constants } from '../../constants';
import { 
  EVM_CHAIN_DEFAULT_TESTNET as defaultTestnet,
  EVM_CHAIN_NETWORK_TO_CHAIN_ID as chainIds 
} from '../../constants/chains';
import { Key } from '../../derivation';
import { MULTISENDAbi } from '../erc20/abi';
import BN from 'bn.js'
const utils = require('web3-utils');
export class ETHTxProvider {
  chain: string;

  constructor(chain = 'ETH') {
    this.chain = chain;
  }

  create(params: {
    recipients: Array<{ address: string; amount: string }>;
    nonce: number;
    gasPrice?: number;
    data: string;
    gasLimit: number;
    network: string;
    chainId?: number;
    contractAddress?: string;
    maxGasFee?: number;
    priorityGasFee?: number;
  }) {
    const { recipients, nonce, gasPrice, gasLimit, network, contractAddress, maxGasFee, priorityGasFee } = params;
    let { data } = params;
    let to;
    let amount;
    if (recipients.length > 1) {
      if (!contractAddress) {
        throw new Error('Multiple recipients requires use of multi-send contract, please specify contractAddress');
      }
      const addresses = [];
      const amounts = [];
      amount = new BN(0);
      for (let recipient of recipients) {
        addresses.push(recipient.address);
        amounts.push(new BN(recipient.amount));
        amount = amount.add(new BN(recipient.amount));
      }
      const multisendContract = this.getMultiSendContract(contractAddress);
      data = data || multisendContract.methods.sendEth(addresses, amounts).encodeABI();
      to = contractAddress;
    } else {
      to = recipients[0].address;
      amount = recipients[0].amount;
    }
    let { chainId } = params;
    chainId = chainId || this.getChainId(network);
    let txData: any = {
      nonce: utils.toHex(nonce),
      gasLimit: utils.toHex(gasLimit),
      to,
      data,
      value: utils.toHex(amount),
      chainId
    };
    if (maxGasFee) {
      txData.maxFeePerGas = utils.toHex(maxGasFee);
      txData.maxPriorityFeePerGas = utils.toHex(priorityGasFee || this.getPriorityFeeMinimum(chainId));
      txData.type = 2;
    } else {
      txData.gasPrice = utils.toHex(gasPrice);
    }

    return ethers.Transaction.from(txData).serialized;
  }

  getMultiSendContract(tokenContractAddress: string) {
    const web3 = new Web3();
    return new web3.eth.Contract(MULTISENDAbi as AbiItem[], tokenContractAddress);
  }

  getPriorityFeeMinimum(chainId: number) {
    const chain = Constants.EVM_CHAIN_ID_TO_CHAIN[chainId];
    return Constants.FEE_MINIMUMS[chain]?.priority || 0;
  }

  getChainId(network: string) {
    if (network === 'testnet') {
      network = defaultTestnet[this.chain];
    }
    return chainIds[`${this.chain}_${network}`] || chainIds[`${this.chain}_mainnet`];
  }

  getSignatureObject(params: { tx: string; key: Key }) {
    const { tx, key } = params;
    // To complain with new ethers
    let k = key.privKey;
    if (k.substr(0, 2) != '0x') {
      k = '0x' + k;
    }

    const signingKey = new ethers.SigningKey(k);
    return signingKey.sign(ethers.keccak256(tx));
  }

  getSignature(params: { tx: string; key: Key }) {
    const signatureHex = ethers.Signature.from(this.getSignatureObject(params));
    return signatureHex;
  }

  getHash(params: { tx: string }) {
    const { tx } = params;
    // tx must be signed, for hash to exist
    return ethers.Transaction.from(tx).hash;
  }

  applySignature(params: { tx: string; signature: any }) {
    let { tx, signature } = params;
    const parsedTx = ethers.Transaction.from(tx);
    const { nonce, gasPrice, gasLimit, to, value, data, chainId, maxFeePerGas, maxPriorityFeePerGas } = parsedTx;
    let txData: any = { nonce, gasPrice, gasLimit, to, value, data, chainId };
    if (maxFeePerGas) {
      txData.maxFeePerGas = maxFeePerGas;
      txData.maxPriorityFeePerGas = maxPriorityFeePerGas;
      txData.type = 2;
    } else if (!gasPrice || !ethers.toNumber(gasPrice)) {
      throw new Error('either gasPrice or maxFeePerGas is required');
    }
    if (typeof signature == 'string') {
      signature = ethers.Signature.from(signature);
    }
    const signedTx = ethers.Transaction.from(txData).serialized;
    const parsedTxSigned = ethers.Transaction.from(signedTx);
    if (!parsedTxSigned.hash) {
      throw new Error('Signature invalid');
    }
    return signedTx;
  }

  sign(params: { tx: string; key: Key }) {
    const { tx, key } = params;
    const signature = this.getSignatureObject({ tx, key });
    return this.applySignature({ tx, signature });
  }
}
