# Set up to run the tests.

  1. copy ../../thoughtcore-test.config.json to ../../thoughtcore.config.json
  2. run mongod
  3. run thought-code's thoughtd (tested with version v0.19) with:
      `./thoughtd -regtest -rpcpassword=thoughtcorenodetest -rpcuser=local321 --rpcport=18332 --addresstype=legacy`
