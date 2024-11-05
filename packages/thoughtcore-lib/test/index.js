"use strict";

var should = require("chai").should();
var thoughtcore = require("../");

describe('#versionGuard', function() {
  it('global._thoughtcore should be defined', function() {
    should.equal(global._thoughtcore, thoughtcore.version);
  });

  it('throw an error if version is already defined', function() {
    (function() {
      thoughtcore.versionGuard('version');
    }).should.throw('More than one instance of thoughtcore');
  });
});
