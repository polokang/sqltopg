# sqltopg — SQL Server → PostgreSQL 同步服务

把 SQL Server（`AquariusEmailDB`）的业务数据同步到 PostgreSQL（`AquariusPG`），供 Aquareporter-pro 读取。

在 Aquareporter → PG 迁移的**方案 A** 里，它是核心组件：老站和 adminpage 只写 SQL Server，Pro 双写（先 SQL Server 后 PG），
sqltopg 定时把 SQL Server 全量同步到 PG —— 负责把老站 / adminpage 的改动带进 PG，也是 Pro 双写失败时的自愈机制。
迁移的整体设计、进度与待决策见 `project-docs/common/migration/`。

两种用法：

1. **常驻同步** —— `npm start`（容器里跑 `src/index.js`），按 `SYNC_INTERVAL_MINUTES` 调度（方案 A 定为**每 2 分钟**），
   每轮全量 UPSERT + 孤儿清理，暴露 `/health`。
2. **初始化** —— `npm run init-db` 重建 PG 镜像表，再 `npm run sync` 灌数据（见「重建 PG」）。

---

## 目录

```
src/
│  ── 同步运行时（会打进镜像）─────────────────────────────────────────
├── index.js                 # 调度 + /health 入口
├── sync-simple.js           # 同步主逻辑（全量 UPSERT + 孤儿清理，单事务）
├── derive.js                # ★ 全部派生规则的唯一实现（迁移与同步共用）
├── upserts.js               # ★ 批量 UPSERT 语句（jsonb_to_recordset）
├── syncLog.js               # sync_runs 运行日志（独立连接）
├── dbConfig.js              # ★ 连接串解析的唯一实现
│
│  ── 只在本地跑；Dockerfile 会从镜像里删掉 ───────────────────────────
├── initDatabase.js          # ★ PG schema 的唯一权威：删除并重建镜像表
├── migration-*.js           # 早期的逐表一次性迁移（user / company / controller / assignment）
└── progress.js              # 进度条（只被 migration-* 使用）

migrations/                  # 不能重建的环境用的幂等原地升级脚本（见「原地升级」）
.env.example                 # 带注释的环境变量模板
docker-compose.yml           # 生产部署（镜像 adminpage.azurecr.io/sqltopg）
```

---

## PG 表一览

命名规范：`Aquareporter-pro/docs/migration/05-pg-naming-convention.md`（全部 lower_snake_case、表名复数）。
建表脚本 `src/initDatabase.js` 是唯一权威。

### 镜像表（`init-db` 会删除重建，靠同步灌数据）

| 表 | 数据源 | 说明 |
|---|---|---|
| `companies` | `dbo.CompanyInfomation` | 公司（用真名 `CompanyName`）。**没有 `jw_code`**。`logo_url` 优先取该公司 Owner（`Production=1`）用户的 `UserInfomation.Logo`，再回退 `CompanyLogoName`（`derive.deriveCompanyLogo`） |
| `users` | `dbo.UserInfomation` | `roles` 由 `UserTypeId` 派生；`jw_code` ← `JWCode`；`managed_company_ids`、`avatar_url` 是 Pro 独有数据，`last_login_at` 取两边较晚值（见约定 ④） |
| `controllers` | `dbo.ControllerInfo` | `company_id` 取**设备自己的** `CompanyId`；`billing_email` 取 Owner（`AccManagerId`）的 `Email`，派生值（R-004） |
| `user_controller_assignments` | `dbo.UserControllerAssignment` | 按 `(user_id, unit_id)` 自然键；`permission` 见下 |
| `user_identities` | **无**（Pro 自有） | Passkey / Google 绑定，不同步 |
| `user_mfa_factors` | **无**（Pro 自有） | MFA，不同步 |
| 视图 ×4 | — | `controller_user_permissions`、`active_user_controllers`、`controller_subscription_status`、`controllers_missing_subscription` |

`users`、`controllers`、`user_controller_assignments` 都有 `sync_source`（`'mssql'` / `'pro'`），语义见约定 ③。

### 跨重建保留的表（`CREATE TABLE IF NOT EXISTS`，`init-db` 不清空）

| 表 | 数据源 | 说明 |
|---|---|---|
| `controller_service_logs` | `dbo.ControllerNotes` | **客户可见**的现场服务记录；Pro 自建行 `source='manual'` |
| `controller_service_tasks` | `ControllerInfo.Action` | **客户可见**的派工任务；status / assigned_to 等工作状态同步不碰 |
| `controller_subscriptions` | `ControllerInfo.RenewalInv/Date` | 通信套餐。迁来的记 `kind='legacy'`；`paid_by_user_id` 取设备 owner，无则现金账户 5040 |
| `admin_controller_notes` | `ControllerInfo.Notes` | **仅内部**。按 `-->` 拆成多行；作者从 OperationLog 反推（只覆盖 16.8%） |
| `admin_operation_logs` | `dbo.OperationLog` | **仅内部**审计，一行一个字段改动；不存旧值；登录与新增事件不迁；明文密码抹成 `***` |
| `sync_runs` | 本项目自己写 | 每轮同步一行，见「运行日志」 |

这几张表的 `unit_id` / `user_id` 引用**故意不加外键** —— `init-db` 的 `DROP TABLE controllers CASCADE`
会静默删掉引用它的外键约束。

### 不归本项目管的表（adminpage 自有，`init-db` 不碰）

`admin_roles` · `admin_permissions` · `admin_role_permissions` · `admin_user_roles` · `firmware_update_logs` ·
`product_labels` · `product_serial_counters`。基线脚本在 `adminpage/adminpage-api/prisma/pg/`。

> ⚠️ `admin_` 前缀不代表归属。`admin_controller_notes` / `admin_operation_logs` **是本项目建的**，
> 前缀只是在说"仅内部可见"。它们对应的客户侧表是 `controller_service_logs` / `controller_service_tasks` ——
> 差别只在受众，但那正是最容易出事的地方，写查询前先看清楚用的是哪一张。

> ⚠️ **源库两种时区约定并存。** `OperationLog.OperationTime` 是真 UTC，而 `ControllerInfo` / `UserInfomation` /
> `CompanyInfomation` / `ControllerNotes` 的时间列是**布里斯班墙上时间**（服务器 `GETDATE()` 就是 UTC+10）。
> 取"哪一天"时不分开处理会差一天 —— 见 `derive.js` 的 `utcInstantToBrisbaneDate` / `wallClockToDate`。

### `permission`（SMALLINT）

数字越大权限越高，留空位便于以后插档（如 `VIEWER=20`）。应用代码引用 Pro 的 `domain/permission.ts`，不要硬编码。

| 值 | 名称 | 含义 |
|---|---|---|
| 0 | `READONLY` | 只读 |
| 50 | `NORMAL` | 默认工作权限 |
| 100 | `OWNER` | 设备所有者 |

### 角色派生（`derive.js` 的 `deriveRoles`）

| `UserTypeId` | `roles` | `managed_company_ids` 初值 |
|---|---|---|
| 22 AquariusSuperUser | `['super']` | `[]` |
| 24 CorporateManager | `['manager']` | `[本人 CompanyId]` |
| 其他 | `['operator']` | `[]` |

**没有 `owner` 角色** —— Owner 是 per-controller 关系（`controllers.owner_user_id`），运行时按 `owner_user_id = user_id` 判定。

---

## 同步行为

每轮在**一个事务**里按外键顺序执行，出错整轮 `ROLLBACK`：

```
companies → users → controllers → user_controller_assignments → 回填 companies.manager_user_id
          → controller_service_logs / _tasks / controller_subscriptions → admin_controller_notes / admin_operation_logs
```

每张表都必须明确**删除语义**（新增同步表时也一样）：

| 表 | 源库删除后 PG 怎么办 |
|---|---|
| `users` / `controllers` / `user_controller_assignments` | 孤儿清理，**只删 `sync_source='mssql'` 的行** |
| `companies` | 不删 |
| `controller_service_logs` | ⚠️ 目前不删（待补，方案 A 阶段 0.5） |
| `controller_service_tasks` / `controller_subscriptions` | 不删（按 source 键 UPSERT） |
| `admin_controller_notes` | 源备注段数变少时删掉尾部多出的行；`source='manual'` 不碰 |
| `admin_operation_logs` | 追加型，`DO NOTHING` |

### 必须知道的约定

**① 全部派生规则只写在 `src/derive.js`。** 一次性迁移和定时同步共用同一份实现，否则两边一旦漂移，数据会在每轮同步里翻来翻去。

**② 写入一律批量。** UPSERT 在 `src/upserts.js`，`jsonb_to_recordset` 每批 1000 行，DO UPDATE 带 `IS DISTINCT FROM`（没变化的行不写）。
原来逐行写对着 Azure PG（单次往返约 17 ms）单轮要 9 分钟，改批量后约 2 秒。
⚠️ 本地 Docker 往返 0.1 ms 级，这类问题排练测不出来。

**③ `sync_source`：孤儿清理只删 `'mssql'` 行，UPSERT 命中改回 `'mssql'`**（`sync-simple.js` 不变式 I2）。
- Pro 双写写进 PG 的行标 `'pro'`。这样 Pro 在本轮"读快照"之后才写进 SQL Server 的行不会被当成孤儿删掉（同步竞态），
  只存在于 PG 的授权行（Manager 的告警偏好）也不会被删。
- 下一轮快照里有了它，UPSERT 就把它改回 `'mssql'`（字段没变化也改），此后源库删了它才会被正常清理。
- 长期停在 `'pro'` 的 `users` / `controllers` 行 = Pro 双写漏了权威库，由对账发现。

**④ `users` 上三个同步不能直接覆盖的字段。**
- `managed_company_ids`：SQL Server 没有这个字段，INSERT 时给派生初值，之后只由 Pro 界面维护。
- `avatar_url`：Pro 独有（老库 `UserInfomation.Logo` 是**公司 logo**，进 `companies.logo_url`）。INSERT 时给 NULL，之后同步不碰。
- `last_login_at`：Pro 登录只写 PG，同步取 `GREATEST(PG 现值, 老库值)`。老库 `LastLoginTime` 是布里斯班墙上时间，
  先用 `derive.wallClockToInstant` 换算成真时刻再比较，否则老库的值永远快 10 小时。

**⑤ 镜像表没有 `updated_at` 自动触发器。** `updated_at` 表示源记录的修改时间，由同步带过来；
**应用层对镜像表的 UPDATE 必须自己写 `updated_at = NOW()`**（`initDatabase.js` 第 3 节，台账 R-209）。

**⑥ 同步会覆盖 Pro 只写 PG 的字段。** 已知：`users.password_hash`（Pro 管理员重置密码）、
`controllers.billing_email`（Pro 的账单邮箱编辑）每轮会被源库值盖回（`project-docs/common/migration/04` §3 D1、D2）。

---

## 调度

由 `.env` 的 **`SYNC_INTERVAL_MINUTES`** 决定：

| 值 | 模式 |
|---|---|
| `>0` | **按间隔**，每 N 分钟一次，从上一轮结束起算。**下限 1 分钟**。方案 A 定为 `2` |
| `0`（或留空） | **每天定时**，时刻由 `SYNC_AT_HOUR` + `SYNC_TIMEZONE` 决定 |

- 这个值同时是**双写的自愈窗口**：mirror 写失败后最长要等一个周期才被同步纠正。
- 调成分钟级的前提是生产 PG 已有 `users` / `controllers` 的 `sync_source` 列（`migrations/005`），否则会撞上同步竞态。
- `performSync()` 有 `isRunning` 保护，上一轮没跑完不会重入；用 `setTimeout` 递归而非 `setInterval`，不会累计漂移。
- 手工跑一次：`npm run sync`（记为 `trigger_source='manual'`）。

### 运行日志 `sync_runs`

每轮一行：开始时插入 `status='running'`，结束时更新为 `success` / `failed`，并写入耗时、读/写/删行数、每张表明细（`tables` JSONB）、错误信息。

- **用独立连接、在同步事务之外提交** —— 否则失败回滚时会把那条失败记录一起回滚掉。
- **日志写失败不影响同步** —— `syncLog.js` 里所有异常都吞掉只打日志。
- 启动时把超过 6 小时还是 `running` 的行（进程被杀 / 崩溃遗留）标为 `failed`。
- 启动时和每轮同步后删除超过 `SYNC_RUNS_RETENTION_DAYS`（默认 90）天的已结束记录。每 2 分钟一行 ≈ 每天 720 行。
- `trigger_source`：`scheduled`（按调度）/ `startup`（启动时补跑）/ `manual`（`npm run sync`）。

```sql
-- 最近 10 次
SELECT id, started_at, status, duration_ms, rows_read, rows_written, rows_deleted, trigger_source, error_message
  FROM sync_runs ORDER BY started_at DESC LIMIT 10;

-- 最近一次每张表的明细
SELECT id, jsonb_pretty(tables) FROM sync_runs ORDER BY id DESC LIMIT 1;

-- 上次成功是什么时候（间隔模式下超过 10 分钟就该关注）
SELECT max(finished_at) FROM sync_runs WHERE status = 'success';
```

### 健康检查

`GET http://localhost:3000/health` 返回调度信息（模式、间隔、下次运行时间）、进程内状态（是否在跑、上次错误）
和从 `sync_runs` 读的最近 5 次运行。容器的 `HEALTHCHECK` 也打这个接口。

---

## 环境变量

完整带注释的模板见 [`.env.example`](.env.example)。

```ini
SQL_SERVER_URL=sqlserver://<host>:1433;database=AquariusEmailDB;user=<user>;password=<pw>;trustServerCertificate=true
POSTGRESQL_URL=postgresql://<user>:<pw>@<host>:5432/AquariusPG

SYNC_INTERVAL_MINUTES=2             # >0 = 每 N 分钟（下限 1）；0 = 每天定时
SYNC_AT_HOUR=0                      # 仅 SYNC_INTERVAL_MINUTES=0 时生效
SYNC_TIMEZONE=Australia/Brisbane    # 仅 SYNC_INTERVAL_MINUTES=0 时生效
SYNC_ON_STARTUP=true                # 启动时先补跑一次
SYNC_RUNS_RETENTION_DAYS=90         # sync_runs 保留天数；0 = 不清理
PORT=3000
```

> ⚠️ `SQL_SERVER_URL` 里的 `database=` **决定实际读哪个库** —— 查询用三段式表名 `<database>.dbo.<table>`，库名从这里取。

> ⚠️ **容器部署时不要把这些写进 `docker-compose.yml` 的 `environment:`**。`dotenv` 不覆盖已存在的环境变量，
> 写在那里会让 `.env` 里的同名值被静默忽略。compose 里只留 `PORT`（必须与端口映射一致）。

---

## 重建 PG（`init-db`）

```bash
npm run init-db
```
```bash
npm run sync
```

- **删除重建**：镜像表（`companies`、`users`、`controllers`、`user_controller_assignments`、`user_identities`、`user_mfa_factors`）和 4 个视图。
- **保留**：「跨重建保留的表」一节的 6 张；adminpage 的表完全不碰。
- ⚠️ `users.managed_company_ids` 的人工配置、`user_identities`（passkey）、`user_mfa_factors`、Pro 写的 `'pro'` 授权行都会丢。
  Pro 上线后**不要再重建**，改用 `migrations/` 原地升级。重建前先备份人工配置：
  ```sql
  SELECT user_id, managed_company_ids FROM users WHERE array_length(managed_company_ids, 1) > 1;
  ```
- 重建后表是空的，要立刻 `npm run sync` 灌数据。

`npm run migrate` 是早期的逐表迁移流程（`init-db` + 4 个 `migration-*.js`），只覆盖四张核心表；现在重建后直接跑 `sync` 即可。

## 原地升级（`migrations/`）

给**不能重建**的环境（生产）用，均幂等、可重复执行：

```bash
psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/<文件>.sql
```

| 文件 | 内容 |
|---|---|
| `001_rename_to_snake_case.sql` | 旧 PascalCase schema 改名为 snake_case（历史） |
| `004_admin_notes_and_operation_logs.sql` | `creator` / `accessory_serial_numbers` 改名、`admin_controller_notes`、`admin_operation_logs`、`paid_by_user_id` |
| `005_sync_source_users_controllers.sql` | `users` / `controllers` 加回 `sync_source`（修同步竞态）。**部署新版同步服务前先跑** |
| `006_avatar_and_last_login.sql` | 清掉历史上从 `Logo` 灌进来的 `avatar_url`、按旧口径（快 10 小时）存的 `last_login_at`。**部署新版同步服务后、Pro 上线前跑一次** |
| `archive/` | 已被 `initDatabase.js` 取代的历史脚本，**不要执行**（002 删 `sync_source` 的决定已被 005 反转） |

---

## 部署

生产用 `docker-compose.yml`：镜像 `adminpage.azurecr.io/sqltopg:latest`，`.env` 以只读卷挂进容器，端口 `127.0.0.1:8087 → 3000`。

```bash
docker compose pull
```
```bash
docker compose up -d
```

镜像（`Dockerfile`）只含运行时依赖与 6 个运行时文件，`initDatabase.js` / `migration-*.js` 构建时被删掉 —— 重建和迁移只在本地跑。

Node.js **18+**；依赖 `dotenv` / `mssql` / `pg`。

---

## 常见问题

- **`relation "users" does not exist`** —— 目标库还没初始化，先 `npm run init-db`。
- **`column "sync_source" does not exist`** —— 生产 PG 还没跑 `migrations/005`。
- **外键报错** —— 源数据有孤立引用。同步时找不到父行的外键值置 NULL（授权行则跳过），日志里会打印数量。
- **同步服务启动就退出** —— 先 `npm run sync` 在前台跑一次看完整堆栈；再看 `sync_runs.error_message`。
