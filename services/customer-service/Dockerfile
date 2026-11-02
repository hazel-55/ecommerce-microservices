FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/customer-service

COPY services/customer-service/package.json services/customer-service/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/customer-service/src ./src

USER node

EXPOSE 50052

CMD ["node", "./src/server.js"]