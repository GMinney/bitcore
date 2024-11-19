
var startGulp = require('thoughtcore-build');

function defaultTask(cb) {
    startGulp('mnemonic')
    cb();
  }
  
  exports.default = defaultTask