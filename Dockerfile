FROM node:22-slim
WORKDIR /app
COPY package.json package-lock.json ./
# embedded-postgres and the Playwright browser are only for running outside
# Docker; the image talks to the compose "db" service instead.
RUN npm ci --no-audit --no-fund --omit=optional
COPY . .
ENV NODE_ENV=production
EXPOSE 8080
CMD ["npx", "tsx", "src/server.ts"]
