FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/order-service

COPY services/order-service/package.json services/order-service/package-lock.json ./
COPY services/order-service/scripts/patch-kafkajs-timer.js ./scripts/patch-kafkajs-timer.js

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/order-service/src ./src

USER node

EXPOSE 50054

CMD ["node", "./src/server.js"]