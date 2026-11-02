FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/product-service

COPY services/product-service/package.json services/product-service/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/product-service/src ./src

USER node

EXPOSE 50053

CMD ["node", "./src/server.js"]