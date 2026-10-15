-- =============================================================================
-- 005_sync_source_users_controllers.sql
--
-- 方案 A 阶段 0.2（2026-10-14，见 project-docs/common/migration/02 §2）：
-- users / controllers 重新加回 sync_source，修同步竞态。
--
--   sqltopg 每轮"先读快照 → UPSERT → 孤儿清理"。Pro 双写在"读快照"之后新建的
--   用户 / 设备不在快照里，旧的无条件清理会把它当孤儿删掉（users 还会 CASCADE
--   掉它的授权行），直到下一轮才恢复。同步间隔调到分钟级后会偶发。
--
--   新规则（不变式 I2，见 src/sync-simple.js 头部）：
--     · Pro 写入标 sync_source='pro'（PgWriteGateway / controllersService 已经这么写）；
--     · 孤儿清理只删 'mssql' 行；
--     · UPSERT 命中时改回 'mssql'（src/upserts.js）。
--
-- ⚠️ 与 user_controller_assignments 已有的那一列定义完全一致。
-- ⚠️ archive/002 曾删掉这两列 —— 本脚本是对那次决定的反转，不要再跑 002。
--
-- 和 004 一样：新环境走 `npm run init-db` 即可，本文件只给不能重建的环境做原地升级。
-- 幂等：可重复执行。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/005_sync_source_users_controllers.sql
--
-- 部署顺序：先跑本脚本，再部署新版 sqltopg（新版的 UPSERT / DELETE 引用了这一列）。
-- =============================================================================

BEGIN;

ALTER TABLE users       ADD COLUMN IF NOT EXISTS sync_source VARCHAR(10) NOT NULL DEFAULT 'mssql';
ALTER TABLE controllers ADD COLUMN IF NOT EXISTS sync_source VARCHAR(10) NOT NULL DEFAULT 'mssql';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_sync_source_check') THEN
    ALTER TABLE users
      ADD CONSTRAINT users_sync_source_check CHECK (sync_source IN ('mssql', 'pro'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'controllers_sync_source_check') THEN
    ALTER TABLE controllers
      ADD CONSTRAINT controllers_sync_source_check CHECK (sync_source IN ('mssql', 'pro'));
  END IF;
END $$;

COMMIT;

-- 自检：两列都应存在、全部是 'mssql'（Pro 尚未双写时）
SELECT 'users' AS tbl, sync_source, count(*) FROM users GROUP BY sync_source
UNION ALL
SELECT 'controllers', sync_source, count(*) FROM controllers GROUP BY sync_source
UNION ALL
SELECT 'user_controller_assignments', sync_source, count(*) FROM user_controller_assignments GROUP BY sync_source
ORDER BY 1, 2;
