'use strict';

var should = require('chai').should();
var expect = require('chai').expect;

var thoughtcore = require('..');
var errors = thoughtcore.errors;
var Unit = thoughtcore.Unit;

describe('Unit', function () {

  it('can be created from a number and unit', function () {
    expect(function () {
      return new Unit(1.2, 'THT');
    }).to.not.throw();
  });

  it('can be created from a number and exchange rate', function () {
    expect(function () {
      return new Unit(1.2, 350);
    }).to.not.throw();
  });

  it('no "new" is required for creating an instance', function () {
    expect(function () {
      return Unit(1.2, 'THT');
    }).to.not.throw();

    expect(function () {
      return Unit(1.2, 350);
    }).to.not.throw();
  });

  it('has property accesors "THT", "mTHT", "uTHT", "bits", and "notions"', function () {
    var unit = new Unit(1.2, 'THT');
    unit.THT.should.equal(1.2);
    unit.mTHT.should.equal(1200);
    unit.uTHT.should.equal(1200000);
    unit.bits.should.equal(1200000);
    unit.notions.should.equal(120000000);
  });

  it('a string amount is allowed', function () {
    var unit;

    unit = Unit.fromTHT('1.00001');
    unit.THT.should.equal(1.00001);

    unit = Unit.fromMilis('1.00001');
    unit.mTHT.should.equal(1.00001);

    unit = Unit.fromMillis('1.00001');
    unit.mTHT.should.equal(1.00001);

    unit = Unit.fromBits('100');
    unit.bits.should.equal(100);

    unit = Unit.fromNotions('8999');
    unit.notions.should.equal(8999);

    unit = Unit.fromFiat('43', 350);
    unit.THT.should.equal(0.12285714);
  });

  it('should have constructor helpers', function () {
    var unit;

    unit = Unit.fromTHT(1.00001);
    unit.THT.should.equal(1.00001);

    unit = Unit.fromMilis(1.00001);
    unit.mTHT.should.equal(1.00001);

    unit = Unit.fromBits(100);
    unit.bits.should.equal(100);

    unit = Unit.fromNotions(8999);
    unit.notions.should.equal(8999);

    unit = Unit.fromFiat(43, 350);
    unit.THT.should.equal(0.12285714);
  });

  it('converts to notions correctly', function () {
    /* jshint maxstatements: 25 */
    var unit;

    unit = Unit.fromTHT(1.3);
    unit.mTHT.should.equal(1300);
    unit.bits.should.equal(1300000);
    unit.notions.should.equal(130000000);

    unit = Unit.fromMilis(1.3);
    unit.THT.should.equal(0.0013);
    unit.bits.should.equal(1300);
    unit.notions.should.equal(130000);

    unit = Unit.fromBits(1.3);
    unit.THT.should.equal(0.0000013);
    unit.mTHT.should.equal(0.0013);
    unit.notions.should.equal(130);

    unit = Unit.fromNotions(3);
    unit.THT.should.equal(0.00000003);
    unit.mTHT.should.equal(0.00003);
    unit.bits.should.equal(0.03);
  });

  it('takes into account floating point problems', function () {
    var unit = Unit.fromTHT(0.00000003);
    unit.mTHT.should.equal(0.00003);
    unit.bits.should.equal(0.03);
    unit.notions.should.equal(3);
  });

  it('exposes unit codes', function () {
    should.exist(Unit.THT);
    Unit.THT.should.equal('THT');

    should.exist(Unit.mTHT);
    Unit.mTHT.should.equal('mTHT');

    should.exist(Unit.bits);
    Unit.bits.should.equal('bits');

    should.exist(Unit.notions);
    Unit.notions.should.equal('notions');
  });

  it('exposes a method that converts to different units', function () {
    var unit = new Unit(1.3, 'THT');
    unit.to(Unit.THT).should.equal(unit.THT);
    unit.to(Unit.mTHT).should.equal(unit.mTHT);
    unit.to(Unit.bits).should.equal(unit.bits);
    unit.to(Unit.notions).should.equal(unit.notions);
  });

  it('exposes shorthand conversion methods', function () {
    var unit = new Unit(1.3, 'THT');
    unit.toTHT().should.equal(unit.THT);
    unit.toMilis().should.equal(unit.mTHT);
    unit.toMillis().should.equal(unit.mTHT);
    unit.toBits().should.equal(unit.bits);
    unit.toNotions().should.equal(unit.notions);
  });

  it('can convert to fiat', function () {
    var unit = new Unit(1.3, 350);
    unit.atRate(350).should.equal(1.3);
    unit.to(350).should.equal(1.3);

    unit = Unit.fromTHT(0.0123);
    unit.atRate(10).should.equal(0.12);
  });

  it('toString works as expected', function () {
    var unit = new Unit(1.3, 'THT');
    should.exist(unit.toString);
    unit.toString().should.be.a('string');
  });

  it('can be imported and exported from/to JSON', function () {
    var json = JSON.stringify({ amount: 1.3, code: 'THT' });
    var unit = Unit.fromObject(JSON.parse(json));
    JSON.stringify(unit).should.deep.equal(json);
  });

  it('importing from invalid JSON fails quickly', function () {
    expect(function () {
      return Unit.fromJSON('¹');
    }).to.throw();
  });

  it('inspect method displays nicely', function () {
    var unit = new Unit(1.3, 'THT');
    unit.inspect().should.equal('<Unit: 130000000 notions>');
  });

  it('fails when the unit is not recognized', function () {
    expect(function () {
      return new Unit(100, 'USD');
    }).to.throw(errors.Unit.UnknownCode);
    expect(function () {
      return new Unit(100, 'THT').to('USD');
    }).to.throw(errors.Unit.UnknownCode);
  });

  it('fails when the exchange rate is invalid', function () {
    expect(function () {
      return new Unit(100, -123);
    }).to.throw(errors.Unit.InvalidRate);
    expect(function () {
      return new Unit(100, 'THT').atRate(-123);
    }).to.throw(errors.Unit.InvalidRate);
  });

});
