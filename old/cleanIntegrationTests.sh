#!/bin/bash

docker container stop $(docker container ls -qf name=thoughtcore_*)
docker container rm $(docker container ls -aqf name=thoughtcore_*)
docker image rm thoughtcore-test_runner
docker image rm thoughtcore-rippled
$(dirname "$(readlink -f "$0")")/packages/thoughtcore-client/bin/wallet-delete --name EthereumWallet-Ci
$(dirname "$(readlink -f "$0")")/packages/thoughtcore-client/bin/wallet-delete --name PolygonWallet-Ci
