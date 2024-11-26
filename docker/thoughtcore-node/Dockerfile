# Dockerfile for thoughtcore-node build found in docker-compose.yml

FROM node:18-bullseye

# Install Chrome

RUN echo 'deb http://dl.google.com/linux/chrome/deb/ stable main' > /etc/apt/sources.list.d/chrome.list

RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -

RUN set -x \
    && apt-get update \
    && apt-get install -y \
    google-chrome-stable

ENV CHROME_BIN /usr/bin/google-chrome

# Log versions

RUN set -x \
    && node -v \
    && npm -v \
    && google-chrome --version


RUN npm i -g npm@10.9.3

WORKDIR /thoughtcore

# Add source
# COPY lerna.json ./

COPY package*.json ./

COPY  ./packages/thoughtcore-client/package.json ./packages/thoughtcore-client/package.json
COPY  ./packages/thoughtcore-client/package-lock.json ./packages/thoughtcore-client/package-lock.json

COPY  ./packages/thoughtcore-build/package.json ./packages/thoughtcore-build/package.json
COPY  ./packages/thoughtcore-build/package-lock.json ./packages/thoughtcore-build/package-lock.json

COPY  ./packages/thoughtcore-lib/package.json ./packages/thoughtcore-lib/package.json
COPY  ./packages/thoughtcore-lib/package-lock.json ./packages/thoughtcore-lib/package-lock.json

COPY  ./packages/thoughtcore-mnemonic/package.json ./packages/thoughtcore-mnemonic/package.json
COPY  ./packages/thoughtcore-mnemonic/package-lock.json ./packages/thoughtcore-mnemonic/package-lock.json

COPY  ./packages/thoughtcore-node/package.json ./packages/thoughtcore-node/package.json
COPY  ./packages/thoughtcore-node/package-lock.json ./packages/thoughtcore-node/package-lock.json

COPY  ./packages/thoughtcore-p2p/package.json ./packages/thoughtcore-p2p/package.json
COPY  ./packages/thoughtcore-p2p/package-lock.json ./packages/thoughtcore-p2p/package-lock.json

COPY  ./packages/thoughtcore-wallet-client/package.json ./packages/thoughtcore-wallet-client/package.json
COPY  ./packages/thoughtcore-wallet-client/package-lock.json ./packages/thoughtcore-wallet-client/package-lock.json

COPY  ./packages/thoughtcore-wallet-service/package.json ./packages/thoughtcore-wallet-service/package.json
COPY  ./packages/thoughtcore-wallet-service/package-lock.json ./packages/thoughtcore-wallet-service/package-lock.json

COPY  ./packages/thoughtcore-wallet/package.json ./packages/thoughtcore-wallet/package.json
COPY  ./packages/thoughtcore-wallet/package-lock.json ./packages/thoughtcore-wallet/package-lock.json

COPY  ./packages/crypto-wallet-core/package.json ./packages/crypto-wallet-core/package.json
COPY  ./packages/crypto-wallet-core/package-lock.json ./packages/crypto-wallet-core/package-lock.json



RUN npm install --ignore-scripts
# Context is handled by docker-compose.yml in monorepo root
ADD . .
RUN npm run compile
