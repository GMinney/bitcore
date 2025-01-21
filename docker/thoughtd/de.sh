#!/bin/bash
set -e

	mkdir -p "$BITCOIN_DATA"


	cat <<-EOF > "$BITCOIN_DATA/thought.conf"
	printtoconsole=1
	rpcallowip=::/0
        server=1
        masternode=1
        externalip=$EXT_IP:10618
        masternodeprivkey=$PRIV_KEY
        masternodeblsprivkey=$BLS_PRIV_KEY
	EOF
	chown bitcoin:bitcoin "$BITCOIN_DATA/thought.conf"

	# ensure correct ownership and linking of data directory
	# we do not update group ownership here, in case users want to mount
	# a host directory and still retain access to it
	chown -R bitcoin "$BITCOIN_DATA"
	ln -sfn "$BITCOIN_DATA" /home/bitcoin/.thoughtcore
	chown -h bitcoin:bitcoin /home/bitcoin/.thoughtcore

	exec gosu bitcoin "$@"
else
	exec "$@"
fi
