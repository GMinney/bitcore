# syntax=docker/dockerfile:1

ARG NODE_VERSION=18.0.0

FROM node:${NODE_VERSION}-alpine

# Use production node environment by default.
# ENV NODE_ENV production


WORKDIR /insight

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

# Run the application as a non-root user.
USER insight

# Copy the rest of the source files into the image. Context is handled by docker-compose.yml in monorepo root
COPY . .

# Expose the port that the application listens on.
# EXPOSE 3000

# Set environment variables for nextjs
# ENV PORT=3000

# server.js is created by next build from the standalone output
# https://nextjs.org/docs/pages/api-reference/next-config-js/output
# ENV HOSTNAME="0.0.0.0"

# Run the application.
CMD node src/index.js