'use strict';

var thoughtcore = module.exports;

// module information
thoughtcore.version = 'v' + require('./package.json').version;
thoughtcore.versionGuard = function(version) {
  if (version !== undefined) {
    var message = 'More than one instance of thoughtcore-lib found. ' +
      'Please make sure to require thoughtcore-lib and check that submodules do' +
      ' not also include their own thoughtcore-lib dependency.';
    throw new Error(message);
  }
};
thoughtcore.versionGuard(global._bitcore);
global._bitcore = thoughtcore.version;

// crypto
thoughtcore.crypto = {};
thoughtcore.crypto.BN = require('./lib/crypto/bn');
thoughtcore.crypto.ECDSA = require('./lib/crypto/ecdsa');
thoughtcore.crypto.Schnorr = require('./lib/crypto/schnorr');
thoughtcore.crypto.Hash = require('./lib/crypto/hash');
thoughtcore.crypto.Random = require('./lib/crypto/random');
thoughtcore.crypto.Point = require('./lib/crypto/point');
thoughtcore.crypto.Signature = require('./lib/crypto/signature');
thoughtcore.crypto.TaggedHash = require('./lib/crypto/taggedhash');

// encoding
thoughtcore.encoding = {};
thoughtcore.encoding.Base58 = require('./lib/encoding/base58');
thoughtcore.encoding.Base58Check = require('./lib/encoding/base58check');
thoughtcore.encoding.BufferReader = require('./lib/encoding/bufferreader');
thoughtcore.encoding.BufferWriter = require('./lib/encoding/bufferwriter');
thoughtcore.encoding.Varint = require('./lib/encoding/varint');

// utilities
thoughtcore.util = {};
thoughtcore.util.buffer = require('./lib/util/buffer');
thoughtcore.util.js = require('./lib/util/js');
thoughtcore.util.preconditions = require('./lib/util/preconditions');

// errors thrown by the library
thoughtcore.errors = require('./lib/errors');

// main bitcoin library
thoughtcore.Address = require('./lib/address');
thoughtcore.Block = require('./lib/block');
thoughtcore.MerkleBlock = require('./lib/block/merkleblock');
thoughtcore.BlockHeader = require('./lib/block/blockheader');
thoughtcore.HDPrivateKey = require('./lib/hdprivatekey.js');
thoughtcore.HDPublicKey = require('./lib/hdpublickey.js');
thoughtcore.Message = require('./lib/message');
thoughtcore.Networks = require('./lib/networks');
thoughtcore.Opcode = require('./lib/opcode');
thoughtcore.PrivateKey = require('./lib/privatekey');
thoughtcore.PublicKey = require('./lib/publickey');
thoughtcore.Script = require('./lib/script');
thoughtcore.Transaction = require('./lib/transaction');
thoughtcore.URI = require('./lib/uri');
thoughtcore.Unit = require('./lib/unit');

// dependencies, subject to change
thoughtcore.deps = {};
thoughtcore.deps.bnjs = require('bn.js');
thoughtcore.deps.bs58 = require('bs58');
thoughtcore.deps.Buffer = Buffer;
thoughtcore.deps.elliptic = require('elliptic');
thoughtcore.deps._ = require('lodash');

// Internal usage, exposed for testing/advanced tweaking
thoughtcore.Transaction.sighash = require('./lib/transaction/sighash');
