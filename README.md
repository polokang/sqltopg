# sqltopg — SQL Server 到 PostgreSQL 数据同步工具

把 SQL Server（`AquariusEmailDB`）中的业务数据搬运到 PostgreSQL（`AquariusPG`），并可作为常驻服务持续同步。

项目提供两种用法：

1. **一次性迁移** —— 用 `migration-*.js` 脚本一次性搬完数据（推荐用于首次初始化）。
2. **定时同步** —— `index.js` 启动一个常驻进程，**每天布里斯班时间 00:00** 做一次全量 UPSERT + 孤儿清理，并暴露 HTTP 健康检查。

---

## 目录结构

```
src/
├── initDatabase.js              # 删除并重建 PG 所有表/视图
├── derive.js                    # ★ 全部派生规则的唯一实现（迁移与同步共用）
├── syncLog.js                   # 同步运行日志（写 sync_runs 表，独立连接）
├── migration-user.js            # 迁移 UserInfomation → users
├── migration-company.js         # 迁移 CompanyInfomation → companies
├── migration-controller.js      # 迁移 ControllerInfo → controllers
├── migration-assignment.js      # 迁移 UserControllerAssignment → user_controller_assignments
├── progress.js                  # 进度条输出工具
├── sync-simple.js               # 常驻同步逻辑（UPSERT + 孤儿清理）
└── index.js                     # 定时任务 + 健康检查服务入口
Dockerfile                       # 运行定时同步服务的镜像
```

---

## 目标 Schema（PostgreSQL，全部 lower_snake_case）

命名规范见 `Aquareporter-pro/docs/migration/05-pg-naming-convention.md`。
建表脚本 `src/initDatabase.js` 是唯一权威。

| 表 / 视图 | 数据源 | 说明 |
|---|---|---|
| `users` | `dbo.UserInfomation` | 用户。`roles` 由 `UserTypeId` 派生；`jw_code` ← `JWCode` |
| `companies` | `dbo.CompanyInfomation` | 公司（185 家，用真名）。**没有 `jw_code`** |
| `controllers` | `dbo.ControllerInfo` | 设备。`company_id` 取**设备自己的**公司 |
| `user_controller_assignments` | `dbo.UserControllerAssignment` | 授权。按 `(user_id, unit_id)` 自然键 |
| `user_identities` | **无**（Pro 自有） | Passkey / Google 绑定，不同步 |
| `user_mfa_factors` | **无**（Pro 自有） | MFA，不同步 |
| 视图 `controller_user_permissions` | — | 授权 ⋈ 用户 ⋈ 设备 |
| 视图 `active_user_controllers` | — | 仅激活用户 × 激活设备 |
| `sync_runs` | 本项目自己写 | 每次同步一行。**不被 `init-db` 清空** —— 重建镜像表不该丢掉运维历史 |

不由本项目管理的表（adminpage 自有）：`admin_roles` · `admin_permissions` ·
`admin_role_permissions` · `admin_user_roles` · `firmware_update_logs`。
它们的 schema 基线在 `adminpage/adminpage-api/prisma/pg/`。

## 调度与运行日志

**每天布里斯班时间 00:00 一次。** 相关环境变量：

```ini
SYNC_TIMEZONE=Australia/Brisbane   # IANA 时区名
SYNC_AT_HOUR=0                     # 0 = 午夜
SYNC_ON_STARTUP=true               # 服务启动时先补一次（默认开，部署完立刻知道通不通）
```

用 `setTimeout` 递归而不是 `setInterval`：每轮结束后按时区重新计算下一次，
不会累计漂移。昆士兰不实行夏令时（全年 UTC+10），但代码按 IANA 时区名算，
换到有夏令时的时区也正确。

想立刻手工跑一次：`npm run sync`（记为 `trigger_source = 'manual'`）。

### `sync_runs` —— 每次同步的记录

```sql
-- 最近 10 次跑得怎么样
SELECT id, started_at, status, duration_ms, rows_read, rows_written, rows_deleted, trigger_source
  FROM sync_runs ORDER BY started_at DESC LIMIT 10;

-- 每张表的明细
SELECT id, jsonb_pretty(tables) FROM sync_runs ORDER BY id DESC LIMIT 1;

-- 上次成功是什么时候（超过 26 小时就该告警）
SELECT max(finished_at) FROM sync_runs WHERE status = 'success';
```

三个设计要点：

1. **日志用独立连接、在同步事务之外提交。** 否则同步失败回滚时会把自己那条失败
   记录一并回滚 —— 失败就永远不留痕，正好在最需要日志的时候没有日志。
2. **开始时先插 `status='running'`，结束时更新。** 进程被杀会留下一条永久 `running`
   的行，那本身就是"这次运行崩了"的信号。服务启动时会把超过 6 小时还是 `running`
   的行标记为 failed。
3. **日志写失败绝不让同步失败。** `syncLog.js` 里所有异常都吞掉只打日志。

`/health` 接口也会带上最近 5 次运行 —— 从表里读，比内存计数器可靠（进程重启后
内存归零，表里的历史还在）。

### 两个必须知道的约定

**① 全部派生规则只写在 `src/derive.js` 里。** 一次性迁移和定时同步共用同一份
实现，否则两边一旦漂移，数据会在每轮同步里翻来翻去。

**② 镜像表上没有 `updated_at` 自动时间戳触发器。** 这四张表的 `updated_at` 由源库
的 `LastUpdateDate` 决定。**应用层对它们的 UPDATE 必须自己写 `updated_at = NOW()`**
（见 `initDatabase.js` 第 3 节的说明和台账 R-209）。

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

# 调度（可选，默认值即下方所示）
SYNC_TIMEZONE=Australia/Brisbane   # IANA 时区名
SYNC_AT_HOUR=0                     # 每天几点跑，0 = 午夜
SYNC_ON_STARTUP=true               # 服务启动时先补跑一次
```

> ⚠️ `SQL_SERVER_URL` 里的 `database=` **决定了实际读哪个库** —— 查询用的是
> 三段式表名 `<database>.dbo.<table>`，库名从这里取。以前是硬编码的
> `AquariusEmailDB`，改 `.env` 指向别的库根本不生效且不报错，2026-09-09 已修。

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

- 每天 **布里斯班时间 00:00** 执行一次（见上文"调度与运行日志"）
- 同步模型：**全量 UPSERT + 孤儿清理**，每次运行都是幂等的
- 每天布里斯班 00:00 同步以下四张表：`companies`、`users`、`controllers`、`user_controller_assignments`（按此顺序，外键决定）
- 不同步的：`user_identities`、`user_mfa_factors`（Pro 自有），以及 `users.managed_company_ids`（SQL Server 无此字段，由 Pro 人工维护，同步永不覆盖）
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

- **`relation "users" does not exist`** —— 目标库还没初始化，先跑一次 `npm run init-db` 或 `npm run migrate`。
- **`null value in column "Name"`** —— SQL Server 里对应记录 `UserName`、`Name`、`Email` 全为 NULL；按设计这种行会被丢弃，请检查源数据。
- **FK 约束报错（`CompanyId_fkey` / `OwnerId_fkey` / ...）** —— 通常是源数据有孤立引用。migration 脚本会先诊断并打印出所有孤儿行，再决定是否恢复约束；`sync-simple.js` 在运行时直接把找不到对应父行的 FK 值置 NULL（或跳过整行）。
- **同步服务启动就报错退出** —— 先用 `npm run sync` 在前台跑一次，观察完整堆栈。
