# sqltopg — SQL Server 到 PostgreSQL 数据同步工具

把 SQL Server（`AquariusEmailDB`）中的业务数据搬运到 PostgreSQL（`AquariusPG`），并可作为常驻服务持续同步。

项目提供两种用法：

1. **一次性迁移** —— 用 `migration-*.js` 脚本一次性搬完数据（推荐用于首次初始化）。
2. **定时增量同步** —— `index.js` 启动一个常驻进程，每 2 分钟做一次全量 UPSERT + 孤儿清理，并暴露 HTTP 健康检查。

---

## 目录结构

```
src/
├── initDatabase.js              # 删除并重建 PG 所有表/视图
├── migration-user.js            # 迁移 UserInfomation → "UserInfo"
├── migration-company.js         # 迁移 UserInfomation → "CompanyInfo"
├── migration-controller.js      # 迁移 ControllerInfo → "ControllerInfo"
├── migration-assignment.js      # 迁移 UserControllerAssignment → "UserControllerAssignment"
├── progress.js                  # 进度条输出工具
├── sync-simple.js               # 常驻同步逻辑（UPSERT + 孤儿清理）
└── index.js                     # 定时任务 + 健康检查服务入口
Dockerfile                       # 运行定时同步服务的镜像
```

---

## 目标 Schema（PostgreSQL，全部 PascalCase）

| 表 / 视图                    | 说明                                                           |
| ---------------------------- | -------------------------------------------------------------- |
| `"UserInfo"`                 | 用户信息（合并了老 schema 中的 `users` + `user_profiles`）     |
| `"CompanyInfo"`              | 公司信息                                                       |
| `"UserIdentity"`             | 第三方身份认证绑定（Google/GitHub 等）                         |
| `"UserMfaFactor"`            | 多因子认证因子（TOTP/SMS/...）                                 |
| `"ControllerInfo"`           | 控制器设备信息                                                 |
| `"UserControllerAssignment"` | 用户-控制器权限关系                                            |
| 视图 `"UserFullInfo"`        | UserInfo 的只读投影                                            |
| 视图 `"ControllerUserPermissions"` | 权限关系 + 用户 + 控制器联合视图                         |
| 视图 `"UserControllers"`     | 激活用户可访问的激活控制器                                     |

### `Permission` 字段（SMALLINT）

`UserControllerAssignment.Permission` 为数值，数字越大权限越高，中间留有空位以便未来扩展（例如 `VIEWER=20`、`EDITOR=70`）：

| 值  | 名称       | 含义                 |
| --- | ---------- | -------------------- |
| 0   | `READONLY` | 只读                 |
| 50  | `NORMAL`   | 默认工作权限         |
| 100 | `OWNER`    | 控制器的所有者权限   |

详见 `src/initDatabase.js` 中 `UserControllerAssignment` 建表处的注释。

---

## 环境变量

在项目根目录创建 `.env`：

```bash
# SQL Server 源库
SQL_SERVER_URL=sqlserver://<host>:1433;database=AquariusEmailDB;user=<user>;password=<pw>;trustServerCertificate=true

# PostgreSQL 目标库
POSTGRESQL_URL=postgresql://<user>:<pw>@<host>:5432/AquariusPG

# 同步服务 HTTP 端口（index.js 使用，可选，默认 3000）
PORT=3000
```

---

## 一次性迁移（推荐首次初始化）

```bash
npm install
npm run migrate
```

`migrate` 按顺序执行：

```
initDatabase.js  →  migration-user.js  →  migration-company.js
                 →  migration-controller.js  →  migration-assignment.js
```

也可以单独运行其中一步：

| Script                       | 作用                                                             |
| ---------------------------- | ---------------------------------------------------------------- |
| `npm run init-db`            | **删除并重建**所有表、视图、触发器（⚠ 会清空目标数据库）         |
| `npm run migrate:user`       | 迁移用户 + 角色派生 + Email 去重                                 |
| `npm run migrate:company`    | 迁移公司，暂停 `ManagerId` 外键，并插入 Rare-Enviro 手工数据     |
| `npm run migrate:controller` | 迁移控制器（JOIN 取 CompanyId/BillingEmail，Suburb 拆为 Tags）   |
| `npm run migrate:assignment` | 迁移权限关系（AccessLevel → Permission 数值映射）                |

**角色派生规则**（由 `migration-user.js` 定义）：

- `UserTypeId = 22` → `['super']`，`ManagedCompanyIds = []`
- `Production = 1` → 追加 `['manager', 'owner']`，`ManagedCompanyIds = [CompanyId]`
- `UserTypeId = 24`（且未被上一条覆盖） → 追加 `['manager']`，`ManagedCompanyIds = [CompanyId]`
- 以上都不满足 → `['operator']`

**Name 回退顺序**：`UserName → Name → Email`。`Email` 为 NULL 的记录整条丢弃。

---

## 常驻同步服务

```bash
npm start          # 生产启动
npm run dev        # 监听文件变化启动
npm run sync       # 执行一次 sync 后退出（便于手动触发）
```

- 默认每 **2 分钟**执行一次同步（可在 `src/index.js` 里改 `SYNC_INTERVAL_MS`）
- 同步模型：**全量 UPSERT + 孤儿清理**，每次运行都是幂等的
- 同步以下三张表：`"UserInfo"`、`"ControllerInfo"`、`"UserControllerAssignment"`
- 不同步的表：`"CompanyInfo"`（由 migration 脚本一次性迁移，后续应由应用侧维护）、`"UserIdentity"`、`"UserMfaFactor"`（应用侧数据，不受源库影响）
- 全程单事务，出错 `ROLLBACK`

### 健康检查

启动后访问：

```
GET http://localhost:3000/health
```

返回：

```json
{
  "status": "ok",
  "service": "sql-to-pg-sync",
  "timestamp": "2026-04-22T03:00:00.000Z",
  "sync": {
    "isRunning": false,
    "lastSyncTime": "...",
    "syncCount": 12,
    "lastError": null
  }
}
```

---

## Docker 部署

```bash
docker build -t sqltopg:latest .
docker run -d --name sqltopg -p 3000:3000 --env-file .env sqltopg:latest
```

镜像只包含运行时依赖 (`dotenv` / `mssql` / `pg`)，启动后直接运行 `src/index.js`。

> ⚠ 当前 `Dockerfile` 把 `.env` `COPY` 进了镜像，仅适合个人/内部使用。生产环境建议改为 `--env-file` 或 K8s Secret 注入。

---

## 依赖

```
dotenv ^16.3.1
mssql  ^10.0.1
pg     ^8.20.0
```

Node.js **18+**。

---

## 常见问题排查

- **`relation "UserInfo" does not exist`** —— 目标库还没初始化，先跑一次 `npm run init-db` 或 `npm run migrate`。
- **`null value in column "Name"`** —— SQL Server 里对应记录 `UserName`、`Name`、`Email` 全为 NULL；按设计这种行会被丢弃，请检查源数据。
- **FK 约束报错（`CompanyId_fkey` / `OwnerId_fkey` / ...）** —— 通常是源数据有孤立引用。migration 脚本会先诊断并打印出所有孤儿行，再决定是否恢复约束；`sync-simple.js` 在运行时直接把找不到对应父行的 FK 值置 NULL（或跳过整行）。
- **同步服务启动就报错退出** —— 先用 `npm run sync` 在前台跑一次，观察完整堆栈。
