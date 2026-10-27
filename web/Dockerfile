# ============================================================
# Frontend SismoWatch (React + TypeScript + Vite, dev server)
# ============================================================
FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --no-audit --no-fund

COPY . .

EXPOSE 5173
HEALTHCHECK --interval=15s --timeout=10s --retries=10 \
    CMD wget -q -O /dev/null http://127.0.0.1:5173 || exit 1

CMD ["npm", "run", "dev"]
