-- =============================================================================
-- 007_users_user_name.sql
--
-- 2026-10-15（project-docs/common/migration/06 W2）：users 拆出登录名。
--
--   users.name      显示名 ← UserInfomation.Name（以前是 UserName 优先）
--   users.user_name 登录名 ← UserInfomation.UserName（新列；老站按 CompanyId + UserName 登录）
--
--   以前 name 取 UserName 优先，Pro 改显示名（写老库 Name）会被下一轮同步改回去。
--
-- 本脚本只加列和索引，并把 admin_operation_logs 里历史行的字段名对齐到新口径；
-- 两列的值由下一轮同步填上（新版同步会把 name 改成 Name、user_name 填 UserName）。
--
-- 幂等：可重复执行。能重建的环境直接 `npm run init-db` + `npm run sync` 即可。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/007_users_user_name.sql
--
-- 部署顺序：先跑本脚本 → 再部署新版 sqltopg（UPSERT 引用了 user_name）→ 最后部署新版 Pro
--           （Pro 登录改为按 user_name 查找）。
-- =============================================================================

BEGIN;

ALTER TABLE users ADD COLUMN IF NOT EXISTS user_name VARCHAR(50);
CREATE INDEX IF NOT EXISTS users_user_name_idx ON users (user_name);

-- admin_operation_logs.field 的口径随之变化（derive.js 的 OPERATION_FIELD_RENAMES）：
--   旧：UserName → 'name'，Name → 保持源列名 'Name'
--   新：UserName → 'user_name'，Name → 'name'
-- 已迁入的历史行要一起改，否则同一张表里两套词汇。顺序不能反：先把旧 'name' 改走，再把 'Name' 改成 'name'。
-- 只动同步迁入的行（source='operation_log'）。第一条带 NOT EXISTS 守卫：表里一旦已有
-- 'user_name' 行（本脚本跑过，或新版同步已经写入），就不再执行 —— 否则重跑时会把第二条
-- 刚改好的 'name'（显示名）误改成 'user_name'。所以务必在部署新版同步**之前**跑。
UPDATE admin_operation_logs
   SET field = 'user_name'
 WHERE source = 'operation_log' AND target_type = 'user' AND field = 'name'
   AND NOT EXISTS (SELECT 1 FROM admin_operation_logs x
                    WHERE x.source = 'operation_log' AND x.target_type = 'user' AND x.field = 'user_name');
UPDATE admin_operation_logs
   SET field = 'name'
 WHERE source = 'operation_log' AND target_type = 'user' AND field = 'Name';

COMMIT;
