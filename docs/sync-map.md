# 同步关系总览（SQL Server → PG）

> 一页看清：sqltopg 每轮从 SQL Server 读哪些表、写进 PG 哪些表、删除怎么处理。
> 字段级的派生规则不在这里，见 `src/derive.js`；建表语句见 `src/initDatabase.js`。
>
> **最后核对：2026-10-30**（对照 `src/sync-simple.js` 的 `syncAllTables`）。
> 改了同步范围（新增 / 删除同步表、改读取方式或删除语义）时，请同步更新本文件。

---

## 1. 总体

- **方向**：单向。SQL Server `AquariusEmailDB` → PostgreSQL `AquariusPG`，PG 的改动不会回写。
- **频率**：`SYNC_INTERVAL_MINUTES`，方案 A 定为每 2 分钟。
- **事务**：每轮所有表在**同一个事务**里完成，任何一步出错整轮 `ROLLBACK`。
- **规模**：读 **6 张源表**（`OperationLog` 只用来认备注作者），写 **8 张 PG 表**。

```
SQL Server (AquariusEmailDB)            PostgreSQL (AquariusPG)
────────────────────────────            ─────────────────────────────────────
CompanyInfomation ───────────────────▶  companies
UserInfomation ──────────────────────▶  users
ControllerInfo ──┬───────────────────▶  controllers
                 ├── .Action ────────▶  controller_service_tasks     （客户可见）
                 ├── .RenewalInv/Date ▶ controller_subscriptions
                 └── .Notes ─────────▶  admin_controller_notes       （仅内部）
UserControllerAssignment ────────────▶  user_controller_assignments
ControllerNotes ─────────────────────▶  controller_service_logs      （客户可见）
OperationLog ┈┈（只读，认备注作者）┈┈▶  admin_controller_notes.created_by
```

> **2026-10-30 变更**：`OperationLog → admin_operation_logs` 不再同步。这张表移交 adminpage，
> 由 adminpage 直接写入（结构也换了），见第 5 节。

---

## 2. 按源表看

| SQL Server 源表 | → PG 表 | 读取方式 |
|---|---|---|
| `CompanyInfomation` | `companies` | 全量 |
| `UserInfomation` | `users` | 全量 |
| `ControllerInfo` | `controllers`（主体字段） | 全量 |
| 　↳ `Action` 列 | `controller_service_tasks` | 全量 |
| 　↳ `RenewalInv` / `RenewalDate` 列 | `controller_subscriptions`（`kind='legacy'`） | 全量 |
| 　↳ `Notes` 列 | `admin_controller_notes`（按 `-->` 拆成多行） | 全量 |
| `UserControllerAssignment` | `user_controller_assignments` | 全量 |
| `ControllerNotes` | `controller_service_logs` | 全量 |

辅助读取（不单独产出表）：

- `UserInfomation` 还被 `companies` 用来取 logo（Owner 的 `Logo`），被 `controllers` 用来取 `billing_email`（Owner 的 `Email`）。
- `OperationLog` 会**全量**读一遍改过 `Notes` 的 `MODIFY_CONTROLLER` 记录，用来反推 `admin_controller_notes.created_by`（每段备注是谁写的）。⚠️ adminpage 2026-10-30 起不再写 `OperationLog`，之后新加的备注认不出作者，`created_by` 为 NULL。

---

## 3. 按 PG 表看（执行顺序 + 删除语义）

| 顺序 | PG 表 | 受众 | 源库删了之后 PG 怎么处理 | 函数 |
|---|---|---|---|---|
| 1 | `companies` | — | 不删 | `syncCompanies` |
| 2 | `users` | — | 孤儿清理，只删 `sync_source='mssql'` | `syncUsers` |
| 3 | `controllers` | — | 同上 | `syncControllers` |
| 4 | `user_controller_assignments` | — | 同上（按 `(user_id, unit_id)` 匹配） | `syncAssignments` |
| 5 | 回填 `companies.manager_user_id` | — | — | `syncAllTables` |
| 6 | `controller_service_logs` | **客户可见** | ⚠️ 目前不删（待补，方案 A 阶段 0.5） | `syncServiceRecords` |
| 7 | `controller_service_tasks` | **客户可见** | 不删（按 source 键 UPSERT） | `syncServiceRecords` |
| 8 | `controller_subscriptions` | — | 不删（按 source 键 UPSERT） | `syncServiceRecords` |
| 9 | `admin_controller_notes` | **仅内部** | 段数变少时删尾部多出的行；`source='manual'` 不碰 | `syncAdminRecords` |

`sync_source`：Pro 双写进 PG 的行标 `'pro'`，孤儿清理不会删它；下一轮源库快照里出现后，UPSERT 会把它改回 `'mssql'`。

---

## 4. 不在同步范围内的 PG 表

| 类别 | 表 |
|---|---|
| Pro 自有（SQL Server 无对应） | `user_identities`（Passkey / Google 绑定）、`user_mfa_factors`；`users.managed_company_ids` 这一列 INSERT 给初值，之后**永不覆盖** |
| adminpage 自有（sqltopg 不碰） | `admin_roles`、`admin_permissions`、`admin_role_permissions`、`admin_user_roles`、`firmware_update_logs`、`product_labels`、`product_serial_counters`、**`admin_operation_logs`**（建表脚本都在 `adminpage/adminpage-api/prisma/pg/`） |
| sqltopg 自己的运行日志 | `sync_runs` |
| 尚未实现 | 字典表 `setup_versions`、`user_types`、`controller_plans`、`countries`、`states`（迁移路线图 M8，标为待做） |

---

## 5. `admin_controller_notes` 与 `admin_operation_logs` 的区别

两张表都是**仅内部可见**，但现在归属不同、回答的问题也不同：

| | `admin_controller_notes` | `admin_operation_logs` |
|---|---|---|
| **归属** | sqltopg（同步产出） | **adminpage**（2026-10-30 起，直接写入） |
| **回答的问题** | 这台设备上**人写了什么备注** | **谁、什么时候、对哪个目标、改前改后是什么** |
| **数据从哪来** | `ControllerInfo.Notes`（一个列），每轮同步 | adminpage-api 的 `helpers/audit.ts`，每次修改时写 |
| **一行是** | 一段备注正文 | 一次修改（批量操作按目标拆） |
| **覆盖对象** | 只有设备 | 设备、用户、公司、配件、标签、计数器、照片 |
| **作者 / 时间** | 源列不记作者，只能从 OperationLog 反推 → 仅约 **16.8%** 有 `created_by` | 每行都有 `actor_user_id`、`occurred_at` |
| **改前 / 改后** | — | `old_values` / `new_values`（jsonb，只含改动的字段） |
| **operation_type** | — | 放行这次请求的权限，外键指向 `admin_permissions` |
| **结构说明** | `src/initDatabase.js` 第 13 节 | `adminpage-api/prisma/pg/006_admin_operation_logs.sql` |

### `admin_controller_notes` 丢掉什么

只留**人写的正文**，以下都会被剥掉（`derive.parseControllerNotes`）：

- `->[X changed to Y]` —— Edit 弹窗自动填的字段变更回显。同一次修改在 `admin_operation_logs` 里有完整的改前改后和操作人，这边不重复存
- `SIMCardNo updated to: 8` / `…: 80` / `…: 800` … —— 老站逐键存盘留下的噪音
- 开头的 `New Controller` 固定前缀、剥完只剩空白或标点的段

### 两张表的重叠

改备注的那次修改里也带着 `Notes` 字段，所以 `admin_operation_logs` 里那一行的 `old_values` / `new_values` 会有 `Notes` 的**改前改后全文**：

- 想看"这台设备有哪些备注" → 查 `admin_controller_notes`
- 想看"谁在什么时候改过备注" → 查 `admin_operation_logs WHERE target_type = 'controller' AND new_values ? 'Notes'`

### 别和这两张客户可见的表搞混

| 内部（`admin_`） | 客户可见 | 区别 |
|---|---|---|
| `admin_controller_notes` ← `ControllerInfo.Notes` | `controller_service_logs` ← `dbo.ControllerNotes` | 源表名只差一个位置，受众完全相反 |
| `admin_operation_logs`（adminpage 写） | `controller_service_tasks` ← `ControllerInfo.Action` | — |

---

## 6. 其他坑

- **时区**：`OperationLog.OperationTime` 是真 UTC；`ControllerInfo` / `UserInfomation` / `CompanyInfomation` / `ControllerNotes` 的时间列是**布里斯班墙上时间**（UTC+10）。取"哪一天"见 `derive.js` 的 `utcInstantToBrisbaneDate` / `wallClockToDate`。
- **`admin_` 前缀不代表归属**：`admin_controller_notes` 是 sqltopg 建的；`admin_operation_logs` 和 `admin_roles` 那一组归 adminpage。
- **不要把 `admin_operation_logs` 加回同步**：表结构已经换成 adminpage 的（`old_values` / `new_values`，没有 `field` / `source` 列），旧的同步逻辑写不进去，会让整轮同步事务回滚。
- **`migrations/004`、`007` 里 `admin_operation_logs` 的部分已作废**：004 的建表语句、007 对 `field` 列的 `UPDATE`，在新结构上都会失败。不要在已经执行过 adminpage 006 的库上再跑。

---

## 7. 怎么核对本文件是否过期

```bash
# 每轮实际调用了哪些同步函数
grep -n "await sync" src/sync-simple.js

# 读了 SQL Server 的哪些表
grep -n 'dbo\.' src/sync-simple.js
```

对照第 2、3 节，有出入就以代码为准，并更新本文件和"最后核对"日期。
