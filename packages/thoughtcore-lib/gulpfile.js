'use strict';

var startGulp = require('thoughtcore-build');

function defaultTask(cb) {
  startGulp('lib')
  cb();
}

exports.default = defaultTask