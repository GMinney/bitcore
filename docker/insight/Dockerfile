# Dockerfile for insight build found in docker-compose.yml

FROM node:23-bullseye


# Use production node environment by default.

# ENV NODE_ENV production


# Change working directory

WORKDIR /insight-app


# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.npm to speed up subsequent builds.
# Leverage a bind mounts to package.json and package-lock.json to avoid having to copy them into
# into this layer.

RUN --mount=type=bind,source=package.json,target=package.json \
    --mount=type=bind,source=package-lock.json,target=package-lock.json \
    --mount=type=cache,target=/root/.npm \
    npm ci --omit=dev

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 insight


# Copy the rest of the source files into the image. Context is handled by docker-compose.yml in monorepo root

COPY ./packages/insight ./insight
COPY ./packages/thoughtcore-lib ./thoughtcore-lib
COPY ./packages/thoughtcore-build ./thoughtcore-build


# Change working directory and install

WORKDIR /insight-app/thoughtcore-build
RUN npm install --ignore-scripts


# Change working directory and install

WORKDIR /insight-app/thoughtcore-lib
RUN npm install --ignore-scripts


# Change working directory and install

WORKDIR /insight-app/insight
RUN npm install --ignore-scripts

# Actually build the application

RUN npm run build


# Run the application as a non-root user.

USER insight


# Expose the port that the application listens on.

# EXPOSE 3000


# Set environment variables for nextjs

# ENV PORT=3000
# server.js is created by next build from the standalone output, see https://nextjs.org/docs/pages/api-reference/next-config-js/output
# ENV HOSTNAME="0.0.0.0"


# Run the application.

CMD node src/index.js