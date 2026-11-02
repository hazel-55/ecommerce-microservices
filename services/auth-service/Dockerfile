FROM node:24-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app/services/auth-service

COPY services/auth-service/package.json services/auth-service/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund --foreground-scripts

COPY --chown=node:node proto /app/proto
COPY --chown=node:node services/auth-service/src ./src

USER node

EXPOSE 50051

CMD ["node", "./src/server.js"]