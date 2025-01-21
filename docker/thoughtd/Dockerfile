# Builder image
FROM ubuntu:20.04 AS builder

RUN set -ex \
	&& apt-get update \
	&& apt-get install -qq --no-install-recommends ca-certificates dirmngr gosu gpg wget

ENV THOUGHT_VERSION=0.18.3
ENV THOUGHT_URL=https://github.com/thoughtnetwork/thought-wallet/raw/master/linux/thought-0.18.3/thoughtcore-0.18.3-x86_64-pc-linux-gnu.tar.gz

# install thought binaries
RUN set -ex \
	&& cd /tmp \
	&& wget -qO "thought-$THOUGHT_VERSION.tar.gz" "$THOUGHT_URL" \
	&& mkdir bin \
	&& tar -xzvf "thought-$THOUGHT_VERSION.tar.gz" -C /tmp/bin --strip-components=2 "thoughtcore/bin/thought-cli" "thoughtcore/bin/thoughtd" \
	&& cd bin \
	&& wget -qO gosu "https://github.com/tianon/gosu/releases/download/1.11/gosu-amd64" \
	&& echo "0b843df6d86e270c5b0f5cbd3c326a04e18f4b7f9b8457fa497b0454c4b138d7 gosu" | sha256sum -c -

# Final image
FROM ubuntu:20.04

COPY --from=builder "/tmp/bin" /usr/local/bin

RUN chmod +x /usr/local/bin/gosu \
	&& groupadd -r thought \
	&& useradd -r -m -g thought thought

# create data directory
ENV THOUGHT_DATA=/data
ENV THOUGHT_NETWORK=mainnet

RUN mkdir "$THOUGHT_DATA" \
	&& chown -R thought:thought "$THOUGHT_DATA" \
	&& ln -sfn "$THOUGHT_DATA" /home/thought/.thoughtcore \
	&& chown -h thought:thought /home/thought/.thoughtcore

#VOLUME /data

# Copy Thought chainstate backup and extract
ADD ./docker/thoughtd/thoughtchainstatebackup.tar.gz /data/

# Copy Thought Entrypoint script
COPY ./docker/thoughtd/docker-entrypoint.sh /docker-entrypoint.sh


RUN chown -R thought:thought "$THOUGHT_DATA"
RUN chmod 777 /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh


# Define the entrypoint for the application, aka our startup script to choose the
# binary to run, and populate the appropriate configuration files
ENTRYPOINT ["/docker-entrypoint.sh"]

# Expose the rpc and p2p ports
EXPOSE 10617 10618 11617 11618

# Run the thought daemon by default
CMD ["thoughtd"]