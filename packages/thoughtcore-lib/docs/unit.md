# Unit

Unit is a utility for handling and converting thought units. We strongly recommend to always use notions to represent amount inside your application and only convert them to other units in the front-end.

To understand the need of using the `Unit` class when dealing with unit conversions, see this example:

```javascript
> 81.99 * 100000 // wrong
8198999.999999999
> var thoughtcore = require('thoughtcore-lib');
> var Unit = thoughtcore.Unit;
> Unit.fromMilis(81.99).toNotions() // correct
8199000
```

## Supported units

The supported units are THT, mTHT, bits (micro THTs, uTHT) and notions. The codes for each unit can be found as members of the Unit class.

```javascript
var thtCode = Unit.THT;
var mthtCode = Unit.mTHT;
var uthtCode = Unit.uTHT;
var bitsCode = Unit.bits;
var satsCode = Unit.notions;
```

## Creating units

There are two ways for creating a unit instance. You can instantiate the class using a value and a unit code; alternatively if the unit it's fixed you could you some of the static methods. Check some examples below:

```javascript
var unit;
var amount = 100;

// using a unit code
var unitPreference = Unit.THT;
unit = new Unit(amount, unitPreference);

// using a known unit
unit = Unit.fromTHT(amount);
unit = Unit.fromMilis(amount);
unit = Unit.fromBits(amount);
unit = Unit.fromNotions(amount);
```

## Conversion

Once you have a unit instance, you can check its representation in all the available units. For your convenience the classes expose three ways to accomplish this. Using the `.to(unitCode)` method, using a fixed unit like `.toNotions()` or by using the accessors.

```javascript
var unit;

// using a unit code
var unitPreference = Unit.THT;
value = Unit.fromNotions(amount).to(unitPreference);

// using a known unit
value = Unit.fromTHT(amount).toTHT();
value = Unit.fromTHT(amount).toMilis();
value = Unit.fromTHT(amount).toBits();
value = Unit.fromTHT(amount).toNotions();

// using accessors
value = Unit.fromTHT(amount).THT;
value = Unit.fromTHT(amount).mTHT;
value = Unit.fromTHT(amount).bits;
value = Unit.fromTHT(amount).notions;
```

## Using a fiat currency

The unit class also provides a convenient alternative to create an instance from a fiat amount and the corresponding THT/fiat exchange rate. Any unit instance can be converted to a fiat amount by providing the current exchange rate. Check the example below:

```javascript
var unit, fiat;
var amount = 100;
var exchangeRate = 350;

unit = new Unit(amount, exchangeRate);
unit = Unit.fromFiat(amount, exchangeRate);

fiat = Unit.fromBits(amount).atRate(exchangeRate);
fiat = Unit.fromBits(amount).to(exchangeRate);
```
