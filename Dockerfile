# Node.js 18 Debian slim
#
# ⚠️ 镜像里**不含任何凭据**。SQL_SERVER_URL / POSTGRESQL_URL 必须在运行时用 -e
#    注入（见 azure-pipelines.yml 的 Deploy 阶段）。
#
#    原先这里有一行 `COPY .env .env`，把生产库口令打进了镜像层 —— 镜像一旦推到
#    ACR，任何有 pull 权限的人 `docker save` 就能读出来，而且镜像层不可变、
#    事后删不掉旧 tag 里的。2026-09-09 移除，同时把 .env 重新加回 .dockerignore。
#
# 时区：调度用 Intl.DateTimeFormat + IANA 时区名（Australia/Brisbane），**不依赖**
#    容器的 TZ 环境变量。已验证本基础镜像自带 full ICU（74.2，418 个时区），
#    Australia/Brisbane 解析正确。若换成 alpine 或 small-icu 的镜像，时区会静默
#    退回 UTC —— 调度就会差 10 小时，且没有任何报错。换基础镜像前务必重测。
FROM node:18-slim

WORKDIR /app

COPY package*.json ./

# 只安装生产依赖（--only=production 已废弃，改用 --omit=dev）
RUN npm ci --omit=dev

COPY src ./src

# 创建非 root 用户
RUN groupadd -r nodejs -g 1001 && \
    useradd -r -u 1001 -g nodejs nodejs

USER nodejs

# 仅供健康探针。本服务**没有入站业务流量**，部署时建议只绑到 127.0.0.1
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

CMD ["node", "src/index.js"]
