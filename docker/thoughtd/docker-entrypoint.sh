#!/bin/sh
set -e

	mkdir -p "$THOUGHT_DATA"
	cat <<-EOF > "$THOUGHT_DATA/thought.conf"
	listen=1
	server=1
	irc=1
	upnp=1
	port=10618
	rpcport=10617
	rpcallowip=0.0.0.0/0
	rpcuser=username
	rpcpassword=password
	addnode=phi.thought.live:10618
	${THOUGHT_EXTRA_ARGS}
	EOF
	chown thought:thought "$THOUGHT_DATA/thought.conf"

	# Commented out params
	# printtoconsole=0


	# ensure correct ownership and linking of data directory
	# we do not update group ownership here, in case users want to mount
	# a host directory and still retain access to it
	chown -R thought "$THOUGHT_DATA"
	ln -sfn "$THOUGHT_DATA" /home/thought/.thoughtcore
	chown -h thought:thought /home/thought/.thoughtcore

	exec gosu thought "$@"
else
	exec "$@"
fi