FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/api-gateway

COPY services/api-gateway/package.json services/api-gateway/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/api-gateway/src ./src

USER node

EXPOSE 3001

CMD ["node", "./src/server.js"]