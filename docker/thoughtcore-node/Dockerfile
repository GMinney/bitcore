# syntax=docker.io/docker/dockerfile:1

FROM node:23-alpine AS base

# Dependencies stage
FROM base AS deps
RUN apk add --no-cache libc6-compat
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
# https://nathanfries.com/posts/docker-npm-workspaces/ fix for package-lock.json not found

WORKDIR /app/thoughtcore-node
COPY ./packages/thoughtcore-node/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-lib
COPY ./packages/thoughtcore-lib/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-mnemonic
COPY ./packages/thoughtcore-mnemonic/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-p2p
COPY ./packages/thoughtcore-p2p/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-wallet-client
COPY ./packages/thoughtcore-wallet-client/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-wallet-service
COPY ./packages/thoughtcore-wallet-service/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtcore-client
COPY ./packages/thoughtcore-client/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/crypto-wallet-core
COPY ./packages/crypto-wallet-core/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/crypto-rpc
COPY ./packages/crypto-rpc/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install

WORKDIR /app/thoughtd-rpc
COPY ./packages/thoughtd-rpc/package.json yarn.lock* package-lock.json* pnpm-lock.yaml* .npmrc* ./
RUN npm install


# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app

COPY --from=deps /app/thoughtcore-client/node_modules ./thoughtcore-client/node_modules
COPY --from=deps /app/thoughtcore-client/package-lock.json ./thoughtcore-client/package-lock.json

COPY --from=deps /app/crypto-wallet-core/node_modules ./crypto-wallet-core/node_modules
COPY --from=deps /app/crypto-wallet-core/package-lock.json ./crypto-wallet-core/package-lock.json

COPY --from=deps /app/thoughtcore-node/node_modules ./thoughtcore-node/node_modules
COPY --from=deps /app/thoughtcore-node/package-lock.json ./thoughtcore-node/package-lock.json

COPY --from=deps /app/thoughtcore-lib/node_modules ./thoughtcore-lib/node_modules
COPY --from=deps /app/thoughtcore-mnemonic/node_modules ./thoughtcore-mnemonic/node_modules
COPY --from=deps /app/thoughtcore-p2p/node_modules ./thoughtcore-p2p/node_modules
COPY --from=deps /app/thoughtcore-wallet-client/node_modules ./thoughtcore-wallet-client/node_modules
COPY --from=deps /app/thoughtcore-wallet-service/node_modules ./thoughtcore-wallet-service/node_modules
COPY --from=deps /app/crypto-rpc/node_modules ./crypto-rpc/node_modules
COPY --from=deps /app/thoughtd-rpc/node_modules ./thoughtd-rpc/node_modules

COPY ./packages/thoughtcore-client ./thoughtcore-client
COPY ./packages/crypto-wallet-core ./crypto-wallet-core
COPY ./packages/thoughtcore-node ./thoughtcore-node

COPY ./packages/thoughtcore-lib ./thoughtcore-lib
COPY ./packages/thoughtcore-mnemonic ./thoughtcore-mnemonic
COPY ./packages/thoughtcore-p2p ./thoughtcore-p2p
COPY ./packages/thoughtcore-wallet-client ./thoughtcore-wallet-client
COPY ./packages/thoughtcore-wallet-service ./thoughtcore-wallet-service
COPY ./packages/crypto-rpc ./crypto-rpc
COPY ./packages/thoughtd-rpc ./thoughtd-rpc

WORKDIR /app/crypto-wallet-core

RUN \
  if [ -f yarn.lock ]; then yarn run build; \
  elif [ -f package-lock.json ]; then npm run build; \
  elif [ -f pnpm-lock.yaml ]; then corepack enable pnpm && pnpm run build; \
  else echo "Lockfile not found." && exit 1; \
  fi

WORKDIR /app/thoughtcore-client

RUN \
  if [ -f yarn.lock ]; then yarn run build; \
  elif [ -f package-lock.json ]; then npm run build; \
  elif [ -f pnpm-lock.yaml ]; then corepack enable pnpm && pnpm run build; \
  else echo "Lockfile not found." && exit 1; \
  fi

WORKDIR /app/thoughtcore-node

RUN \
  if [ -f yarn.lock ]; then yarn run build; \
  elif [ -f package-lock.json ]; then npm run build; \
  elif [ -f pnpm-lock.yaml ]; then corepack enable pnpm && pnpm run build; \
  else echo "Lockfile not found." && exit 1; \
  fi


# Production stage
FROM base AS runner
WORKDIR /app/thoughtcore-node

ENV NODE_ENV=production

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 expressjs

COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-node/build/src ./
COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-node/thoughtcore-config.json ./
COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-node/node_modules ./node_modules

COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-lib ./node_modules/thoughtcore-lib
COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-p2p ./node_modules/thoughtcore-p2p
COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-client ./node_modules/thoughtcore-client
COPY --from=builder --chown=expressjs:nodejs /app/crypto-wallet-core ./node_modules/crypto-wallet-core
COPY --from=builder --chown=expressjs:nodejs /app/crypto-rpc ./node_modules/crypto-rpc
COPY --from=builder --chown=expressjs:nodejs /app/thoughtcore-wallet-client ./node_modules/thoughtcore-wallet-client


EXPOSE 3004

ENV PORT=3004

# server.js is created by next build from the standalone output
# https://nextjs.org/docs/pages/api-reference/next-config-js/output

CMD ["node", "server.js"]