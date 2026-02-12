# 使用 Node.js 18 Debian slim 镜像（更好的 Prisma 兼容性）
FROM node:18-slim

# 设置工作目录
WORKDIR /app

# 安装必要的系统依赖（Prisma 需要）
RUN apt-get update && \
    apt-get install -y openssl && \
    rm -rf /var/lib/apt/lists/*

# 复制 package.json 和 package-lock.json
COPY package*.json ./

# 安装依赖
RUN npm ci --only=production

# 复制 Prisma schema
COPY prisma ./prisma

# 生成 Prisma Client
RUN npx prisma generate

# 复制应用代码
COPY src ./src
COPY config ./config

# 复制数据库 schema 文件（用于初始化数据库结构）
COPY database-schema.sql ./

# 复制环境变量文件（注意：包含敏感信息，请谨慎使用）
COPY .env .env

# 创建非 root 用户
RUN groupadd -r nodejs -g 1001 && \
    useradd -r -u 1001 -g nodejs nodejs

# 切换到非 root 用户
USER nodejs

# 暴露端口
EXPOSE 3000

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# 启动应用
CMD ["node", "src/index.js"]

