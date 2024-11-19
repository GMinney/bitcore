'use strict';

var startGulp = require('thoughtcore-build');

function defaultTask(cb) {
  startGulp('p2p', { skipBrowser: true })
  cb();
}

exports.default = defaultTask