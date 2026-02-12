# SQL Server 到 PostgreSQL 数据同步工具

实时同步 SQL Server 数据库中的数据到 PostgreSQL 数据库，支持增量同步和自动表结构创建。

## 功能特点

- ✅ 自动创建 PostgreSQL 表结构
- ✅ 增量同步（只同步变化的记录）
- ✅ 定时同步（每2分钟自动执行）
- ✅ 健康检查端点（端口 3000）
- ✅ Docker 支持

## 同步的表

程序会同步以下 SQL Server 表到 PostgreSQL：

| SQL Server 表 | PostgreSQL 表 | 说明 |
|--------------|--------------|------|
| `UserInfomation` | `users` + `user_profiles` | 用户核心信息和扩展信息 |
| `ControllerInfo` | `controllers` | 控制器（设备）信息 |
| `UserControllerAssignment` | `controller_user_assignments` | 用户-控制器权限关系 |
| `ControllerNotes` | `controller_notes` | 控制器备注记录（厂家修改记录） |

### 表关系说明

- **users**: 用户核心信息（UserId, Email, Name 等）
- **user_profiles**: 用户扩展信息（登录时间、验证状态等）
- **controllers**: 控制器信息（UnitId, SIMCardNo, SystemID 等）
- **controller_user_assignments**: 用户对控制器的权限关系（多对多）
- **controller_notes**: 控制器备注记录（同步自 SQL Server ControllerNotes 表）

## 本地测试

### 1. 安装依赖

```bash
npm install
```

### 2. 配置环境变量

复制 `env.example` 为 `.env`：

```bash
cp env.example .env
```

编辑 `.env` 文件：

```env
# SQL Server 连接字符串
SQL_SERVER_URL=sqlserver://host:1433;database=dbname;user=username;password=password;trustServerCertificate=true

# PostgreSQL 数据库配置
POSTGRESQL_URL=postgresql://username:password@localhost:5432/database_name?schema=public

# 可选：HTTP 服务端口（默认 3000）
PORT=3000
```

### 3. 生成 Prisma Client

```bash
npm run prisma:generate
```

### 4. 运行同步

#### 单次同步（测试用）

```bash
npm run sync
```

#### 启动定时同步服务（每2分钟自动同步）

```bash
npm start
```

服务启动后会：
- 立即执行一次全量同步
- 之后每2分钟自动执行增量同步
- 启动健康检查服务（端口 3000）

#### 检查健康状态

```bash
curl http://localhost:3000/health
```

或在浏览器访问：`http://localhost:3000/health`

## Docker 部署

### 1. 构建镜像

```bash
docker build -t adminpage.azurecr.io/sqltopg:v1.0 .
```

### 2. 推送镜像到 Azure Container Registry

```bash
# 登录（如果需要）
az acr login --name adminpage

# 推送镜像
docker push adminpage.azurecr.io/sqltopg:v1.0
```

### 3. 运行容器

```bash
docker run -d \
  --name sqltopg \
  -p 3000:3000 \
  --env-file .env \
  adminpage.azurecr.io/sqltopg:v1.0
```

> **注意**: `.env` 文件已经打包到镜像中，也可以直接运行（不指定 `--env-file`）。

### 4. 查看日志

```bash
docker logs -f sqltopg
```

### 5. 停止容器

```bash
docker stop sqltopg
docker rm sqltopg
```

## 同步机制

### 首次同步（全量）

- 删除并重建所有表结构
- 清空表并插入所有记录

### 后续同步（增量）

- 自动检测表是否为空
- 比较源数据库和目标数据库的差异
- 只同步新增、更新、删除的记录
- 记录所有变更日志

### 同步频率

- 默认每 **2 分钟**执行一次同步
- 可通过修改 `src/index.js` 中的 `SYNC_INTERVAL_MS` 调整

## 环境变量

| 变量名 | 说明 | 必需 | 默认值 |
|--------|------|------|--------|
| `SQL_SERVER_URL` | SQL Server 连接字符串 | 是 | - |
| `POSTGRESQL_URL` | PostgreSQL 连接字符串 | 是 | - |
| `PORT` | HTTP 服务端口 | 否 | 3000 |

## 项目结构

```
sqltopg/
├── src/
│   ├── index.js              # 主程序入口（定时同步服务）
│   └── sync-simple.js        # 同步逻辑实现
├── prisma/
│   └── schema.prisma         # Prisma schema（仅用于生成 Client，不定义 model）
├── database-schema.sql        # PostgreSQL 表结构定义
├── Dockerfile                 # Docker 镜像构建文件
├── package.json               # 项目依赖
└── README.md                  # 本文档
```

## 注意事项

1. **首次运行**：程序会自动创建所有表结构，无需手动创建
2. **数据完整性**：程序会按外键依赖顺序同步表，确保数据完整性
3. **增量同步**：只有真正有字段变化的记录才会被更新，减少数据库负载
4. **错误处理**：同步失败不会影响其他表的同步，错误会记录到日志中

## 故障排查

### 连接失败

- 检查 `.env` 文件配置是否正确
- 检查网络连接和防火墙设置
- 检查数据库服务是否运行

### 表不存在错误

- 重新运行同步，程序会自动创建表结构
- 检查 `database-schema.sql` 文件是否存在

### 同步不工作

- 查看日志输出
- 检查数据是否被过滤（如无效 UserId）
- 访问健康检查端点查看同步状态
