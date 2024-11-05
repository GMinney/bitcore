# Thoughtcore Mnemonics

BIP39 Mnemonics for thoughtcore

[![NPM Package](https://img.shields.io/npm/v/thoughtcore-mnemonic.svg?style=flat-square)](https://www.npmjs.org/package/thoughtcore-mnemonic)
[![Build Status](https://img.shields.io/travis/thoughtnetwork/thoughtcore-mnemonic.svg?branch=master&style=flat-square)](https://travis-ci.org/thoughtnetwork/thoughtcore-mnemonic)
[![Coverage Status](https://img.shields.io/coveralls/thoughtnetwork/thoughtcore-mnemonic.svg?style=flat-square)](https://coveralls.io/r/thoughtnetwork/thoughtcore-mnemonic)

**A module for [thoughtcore](https://github.com/thoughtnetwork/thoughtcore) that implements [Mnemonic code for generating deterministic keys](https://github.com/thought/bips/blob/master/bip-0039.mediawiki).**

## Getting Started

This library is distributed in both the npm packaging systems.

```sh
npm install thoughtcore-lib  #this to install thoughtcore-lib since it is a peerDependecy
npm install thoughtcore-mnemonic
```

There are many examples of how to use it on the developer guide [section for mnemonic](./docs/index.md). For example, the following code would generate a new random mnemonic code and convert it to a `HDPrivateKey`.

```javascript
var Mnemonic = require('thoughtcore-mnemonic');
var code = new Mnemonic(Mnemonic.Words.SPANISH);
code.toString(); // natal hada sutil año sólido papel jamón combate aula flota ver esfera...
var xpriv = code.toHDPrivateKey();
```

## Contributing

See [CONTRIBUTING.md](https://github.com/thoughtnetwork/thoughtcore/blob/master/CONTRIBUTING.md) on the main thoughtcore repo for information about how to contribute.

## License

Code released under [the MIT license](https://github.com/thoughtnetwork/thoughtcore/blob/master/LICENSE).

Copyright 2013-2019 Thought, Inc. Thoughtcore is a trademark maintained by Thought, Inc.
