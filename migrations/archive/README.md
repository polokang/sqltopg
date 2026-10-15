# 已归档的原地升级脚本

这两个脚本是给**已有 PG 环境做 ALTER 升级**用的。

**现在不再需要**：schema 变更一律走 `npm run init-db` 重建，本地每次都会执行。
所有 schema 定义以 `src/initDatabase.js` 为准，同步逻辑以 `src/sync-simple.js`
和 `src/derive.js` 为准。

保留在这里只为查阅历史 —— **不要执行**，它们的内容已经落后于 initDatabase.js。

| 文件 | 内容 | 已被 initDatabase.js 取代 |
|---|---|---|
| `002_comm_types_and_subscriptions.sql` | comm_types 数组、订阅表与两个视图、去掉 sync_source | ✅ |

> ⚠️ 002 去掉 users / controllers 的 sync_source 这一项已于 2026-10-14 反转（`migrations/005`，方案 A 阶段 0.2 修同步竞态）。
| `003_fix_model_type.sql` | model_type 去 nchar 填充 / 'NONE'→NULL / 统一大写 | ✅（现由 derive.deriveControllerModel 处理） |
