'use strict';

import thoughtcore from 'thoughtcore-lib';
import { Inventory } from '../inventory';

export class builder {
  options: any;

  constructor(options) {

    if (!options) {
      options = {};
    }
  
    if (!options.network) {
      options.network = thoughtcore.Networks.defaultNetwork;
    }
  
    options.Block = options.Block || thoughtcore.Block;
    options.BlockHeader = options.BlockHeader || thoughtcore.BlockHeader;
    options.Transaction = options.Transaction || thoughtcore.Transaction;
    options.MerkleBlock = options.MerkleBlock || thoughtcore.MerkleBlock;
    options.protocolVersion = options.protocolVersion || 70018;

    var exported = {
      constructors: {
        Block: options.Block,
        BlockHeader: options.BlockHeader,
        Transaction: options.Transaction,
        MerkleBlock: options.MerkleBlock
      },
      defaults: {
        protocolVersion: options.protocolVersion,
        network: options.network
      },
      inventoryCommands: [
        'getdata',
        'inv',
        'notfound'
      ],
      commandsMap: {
        version: 'Version',
        verack: 'Verack',
        ping: 'Ping',
        pong: 'Pong',
        block: 'Block',
        tx: 'Transaction',
        getdata: 'GetData',
        headers: 'Headers',
        notfound: 'NotFound',
        inv: 'Inventory',
        addr: 'Addresses',
        alert: 'Alert',
        reject: 'Reject',
        merkleblock: 'MerkleBlock',
        filterload: 'FilterLoad',
        filteradd: 'FilterAdd',
        filterclear: 'FilterClear',
        getblocks: 'GetBlocks',
        getheaders: 'GetHeaders',
        mempool: 'MemPool',
        getaddr: 'GetAddr'
      },
      commands: {}
    };
  
    // exported.add is a function that takes a key and a command, 
    // where the index of 'commands' is another functiion that takes an object and returns an instance of the command
    // where the index of 'commands' contains a property '_constructor' that contains the command constructor
    // where the index of 'commands' contains a function 'fromBuffer' that takes a buffer and returns a new instance of the messsage at that index, the payload is also set
    exported.add = function (key, Command) {
      exported.commands[key] = function (obj) {
        return new Command(obj, options);
      };
  
      exported.commands[key]._constructor = Command;
  
      exported.commands[key].fromBuffer = function (buffer) {
        var message = exported.commands[key]();
        message.setPayload(buffer);
        return message;
      };
    };
  
  
  // for each key in the commandsMap object, add the key and the command to the exported object. so the exported object will be a list of all the commands that reference their source files
  // calls on exported.verack for example will return the VerackMessage constructor
    Object.keys(exported.commandsMap).forEach(function (key) {
      exported.add(key, require('./commands/' + key));
    });
  
    exported.inventoryCommands.forEach(function (command) {
  
      // add forTransaction methods
      exported.commands[command].forTransaction = function forTransaction(hash) {
        return new exported.commands[command]([Inventory.forTransaction(hash)]);
      };
  
      // add forBlock methods
      exported.commands[command].forBlock = function forBlock(hash) {
        return new exported.commands[command]([Inventory.forBlock(hash)]);
      };
  
      // add forFilteredBlock methods
      exported.commands[command].forFilteredBlock = function forFilteredBlock(hash) {
        return new exported.commands[command]([Inventory.forFilteredBlock(hash)]);
      };
  
    });
  
    return exported;
  }




}


