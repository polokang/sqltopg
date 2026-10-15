-- =============================================================================
-- 006_avatar_and_last_login.sql
--
-- 2026-10-15（project-docs/common/migration/04 Q18、Q19）：
--
--   ① users.avatar_url 改为 Pro 独有。老库 UserInfomation.Logo 实际是**公司 logo**
--     （现在进 companies.logo_url），以前被同步成了用户头像。新版同步不再写这一列，
--     这里把历史上从 Logo 灌进来的值清掉。
--
--   ② users.last_login_at 改为取两边较晚值（Pro 登录只写 PG）。以前同步把老库的
--     **布里斯班墙上时间**当 UTC 存进来，比真实时刻快 10 小时；新版同步先换算再比较。
--     旧值若不清掉，GREATEST 会让这些"快 10 小时"的值一直赢下去。清成 NULL 后，
--     下一轮同步会用换算后的老库值重新填上。
--
-- ⚠️ **只在 Pro 正式上线前执行一次。** 上线后再跑会抹掉用户自己上传的头像和 Pro 的登录时间。
--    能重建的环境直接 `npm run init-db` + `npm run sync` 即可，不需要这个文件。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/006_avatar_and_last_login.sql
--
-- 部署顺序：先部署新版 sqltopg，再跑本脚本（反过来的话旧版同步会立刻把值灌回来）。
-- =============================================================================

BEGIN;

UPDATE users SET avatar_url = NULL WHERE avatar_url IS NOT NULL;
UPDATE users SET last_login_at = NULL WHERE last_login_at IS NOT NULL;

COMMIT;
