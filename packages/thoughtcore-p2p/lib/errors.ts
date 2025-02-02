'use strict';

var spec = {
  name: 'P2P',
  message: 'Internal Error on thoughtcore-p2p Module {0}'
};

module.exports = require('thoughtcore-lib').errors.extend(spec);
