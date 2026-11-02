FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/shipment-service

COPY services/shipment-service/package.json services/shipment-service/package-lock.json ./
COPY services/shipment-service/scripts/patch-kafkajs-timer.js ./scripts/patch-kafkajs-timer.js

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/shipment-service/src ./src

USER node

EXPOSE 50055

CMD ["node", "./src/server.js"]