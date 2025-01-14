FROM node:18-alpine

WORKDIR /app
COPY . .

RUN apk add --no-cache --virtual .build-deps git
RUN npm ci && \
    npm run build && \
    npm run generate:git-info && \
    npm prune --production
RUN apk del .build-deps

CMD ["node", "./dist/src/index.js"]
