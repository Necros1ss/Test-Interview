FROM node:24-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY . .

# Run as non-root user for security best practices
USER node

EXPOSE 3001 3002 3003 3004 3005

CMD ["node", "services/order-service/index.js"]
