-- =============================================================================
-- ⛔ 已废弃 —— 不要执行。留档参考用。
--
-- 这个脚本是 2026-09-01 为"原地改名"路线写的：把存量库里 Pro 建的 PascalCase
-- 表/列改成 snake_case。它在 Docker PG 16 上正反跑通过两轮。
--
-- 但 2026-09-09 实际走的是**删表重建**路线（生产库当时并存三代表，且有大量
-- 无人引用的旧数据，重建比原地手术更干净、可回退）。建库请用 src/initDatabase.js。
--
-- ⚠️ 而且本脚本产出的 schema 现在与 src/initDatabase.js **已经不一致**：
--      · companies 仍带 jw_code 列        → 现已删除（Jiwa 码属于 user）
--      · companies.name 仍带 UNIQUE       → 现已去掉（源库确实有重名公司）
--      · 四张镜像表仍建 updated_at 触发器 → 现已移除（源库拥有 updated_at）
--      · users 缺 jw_code 列
--    照它执行会得到一个代码不认的库。
--
-- 保留原因：如果将来遇到"必须原地改名、不能重建"的库，这里的 98 处 RENAME
-- 与约束改名逻辑仍是有效起点 —— 但必须先按上面四条对齐 initDatabase.js。
-- =============================================================================

-- =============================================================================
-- 001_rename_to_snake_case.sql
--
-- 把 AquariusPG 里 Pro 建的 PascalCase 表/列改名为 snake_case，并顺带修掉
-- O1 / O4 / O6 / O7 / O12 几项缺陷。
--
-- 规范依据：Aquareporter-pro/docs/migration/05-pg-naming-convention.md
--
-- 影响范围：只动 Pro 建的 6 张表 + 3 个视图。
--           adminpage 的 admin_user_roles / admin_role_permissions /
--           firmware_update_logs 不受影响，无需停机。
--
-- 执行前：
--   ① 停掉 sqltopg 同步服务（否则改名中途它会报错退出）
--   ② 备份：pg_dump -Fc -t '"User*"' -t '"Company*"' -t '"Controller*"' …
--   ③ 跑一遍第 0 节的体检查询，确认没有会让约束建失败的脏数据
--
-- 执行：psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f 001_rename_to_snake_case.sql
--
-- 回滚：见 001_rename_to_snake_case_rollback.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- ── 0. 执行前体检（只读，不在事务内；有输出就先处理再往下跑）─────────────────
--
-- 0a. permission 是否有 0/50/100 以外的值（会让 CHECK 建失败）
--     SELECT "Permission", count(*) FROM "UserControllerAssignment" GROUP BY 1;
--
-- 0b. email 忽略大小写后是否有重复（会让 lower(email) 唯一索引建失败）
--     SELECT lower("Email"), count(*) FROM "UserInfo"
--      GROUP BY 1 HAVING count(*) > 1;
--
-- 0c. roles 小写化后是否会产生重复元素（数组去重用）
--     SELECT "UserId", "Roles" FROM "UserInfo"
--      WHERE EXISTS (SELECT 1 FROM unnest("Roles") r WHERE r <> lower(r));
-- ---------------------------------------------------------------------------

BEGIN;

-- ── 1. 删除视图 ─────────────────────────────────────────────────────────────
-- 视图会被后面的列改名带着走（PG 存的是解析后的依赖），但视图自己的输出列名
-- 仍是旧的 PascalCase。直接删掉重建更干净。
-- "UserFullInfo" 是 UserInfo 的纯投影，没有抽象价值，删掉不再重建。

DROP VIEW IF EXISTS "UserControllers"           CASCADE;
DROP VIEW IF EXISTS "ControllerUserPermissions" CASCADE;
DROP VIEW IF EXISTS "UserFullInfo"              CASCADE;


-- ── 2. 删除触发器与触发器函数 ───────────────────────────────────────────────
-- 触发器函数体里写死了 NEW."UpdatedAt" / NEW."DateLastUpdated"，改名后必须重写。

DROP TRIGGER  IF EXISTS trg_userinfo_updated_at       ON "UserInfo";
DROP TRIGGER  IF EXISTS trg_companyinfo_updated_at    ON "CompanyInfo";
DROP TRIGGER  IF EXISTS trg_usermfafactor_updated_at  ON "UserMfaFactor";
DROP TRIGGER  IF EXISTS trg_controllerinfo_updated_at ON "ControllerInfo";
DROP TRIGGER  IF EXISTS trg_uca_updated_at            ON "UserControllerAssignment";

DROP FUNCTION IF EXISTS update_updated_at_column()             CASCADE;
DROP FUNCTION IF EXISTS update_controllers_date_last_updated() CASCADE;


-- ── 3. 表改名 ───────────────────────────────────────────────────────────────

ALTER TABLE "UserInfo"                 RENAME TO users;
ALTER TABLE "CompanyInfo"              RENAME TO companies;
ALTER TABLE "ControllerInfo"           RENAME TO controllers;
ALTER TABLE "UserControllerAssignment" RENAME TO user_controller_assignments;
ALTER TABLE "UserIdentity"             RENAME TO user_identities;
ALTER TABLE "UserMfaFactor"            RENAME TO user_mfa_factors;


-- ── 4. 列改名 ───────────────────────────────────────────────────────────────

-- 4.1 users
ALTER TABLE users RENAME COLUMN "UserId"            TO user_id;
ALTER TABLE users RENAME COLUMN "CompanyId"         TO company_id;
ALTER TABLE users RENAME COLUMN "Email"             TO email;
ALTER TABLE users RENAME COLUMN "EmailVerified"     TO email_verified;
ALTER TABLE users RENAME COLUMN "Mobile"            TO mobile;
ALTER TABLE users RENAME COLUMN "MobileVerified"    TO mobile_verified;
ALTER TABLE users RENAME COLUMN "Name"              TO name;
ALTER TABLE users RENAME COLUMN "PasswordHash"      TO password_hash;
ALTER TABLE users RENAME COLUMN "Avatar"            TO avatar_url;
ALTER TABLE users RENAME COLUMN "Roles"             TO roles;
ALTER TABLE users RENAME COLUMN "ManagedCompanyIds" TO managed_company_ids;
ALTER TABLE users RENAME COLUMN "Activated"         TO activated;
ALTER TABLE users RENAME COLUMN "MfaEnabled"        TO mfa_enabled;
ALTER TABLE users RENAME COLUMN "LinkedProviders"   TO linked_providers;
ALTER TABLE users RENAME COLUMN "LastLoginTime"     TO last_login_at;
ALTER TABLE users RENAME COLUMN "CreatedAt"         TO created_at;
ALTER TABLE users RENAME COLUMN "UpdatedAt"         TO updated_at;

-- 4.2 companies
ALTER TABLE companies RENAME COLUMN "CompanyId"   TO company_id;
ALTER TABLE companies RENAME COLUMN "CompanyName" TO name;
ALTER TABLE companies RENAME COLUMN "ManagerId"   TO manager_user_id;
ALTER TABLE companies RENAME COLUMN "JwCode"      TO jw_code;
ALTER TABLE companies RENAME COLUMN "Desc"        TO description;   -- desc 是保留字
ALTER TABLE companies RENAME COLUMN "Phone"       TO phone;
ALTER TABLE companies RENAME COLUMN "Email"       TO email;
ALTER TABLE companies RENAME COLUMN "CompanyLogo" TO logo_url;
ALTER TABLE companies RENAME COLUMN "Activated"   TO activated;
ALTER TABLE companies RENAME COLUMN "CreatedAt"   TO created_at;
ALTER TABLE companies RENAME COLUMN "UpdatedAt"   TO updated_at;

-- 4.3 controllers
ALTER TABLE controllers RENAME COLUMN "UnitId"          TO unit_id;
ALTER TABLE controllers RENAME COLUMN "SerialNo"        TO serial_no;
ALTER TABLE controllers RENAME COLUMN "SimCardNo"       TO sim_card_no;
ALTER TABLE controllers RENAME COLUMN "OwnerId"         TO owner_user_id;
ALTER TABLE controllers RENAME COLUMN "CompanyId"       TO company_id;
ALTER TABLE controllers RENAME COLUMN "SystemId"        TO system_id;
ALTER TABLE controllers RENAME COLUMN "FirmwareVersion" TO firmware_version;
ALTER TABLE controllers RENAME COLUMN "ModelType"       TO model_type;
ALTER TABLE controllers RENAME COLUMN "TimezoneId"      TO timezone_id;
ALTER TABLE controllers RENAME COLUMN "Status"          TO production_status;  -- 语义修正
ALTER TABLE controllers RENAME COLUMN "Activated"       TO activated;
ALTER TABLE controllers RENAME COLUMN "Creator"         TO site_location;      -- 语义修正
ALTER TABLE controllers RENAME COLUMN "Tags"            TO tags;
ALTER TABLE controllers RENAME COLUMN "BillingEmail"    TO billing_email;
ALTER TABLE controllers RENAME COLUMN "BarCode"         TO bar_code;
ALTER TABLE controllers RENAME COLUMN "PoNumber"        TO po_number;
ALTER TABLE controllers RENAME COLUMN "SpareParts"      TO spare_parts;
ALTER TABLE controllers RENAME COLUMN "Notes"           TO notes;
ALTER TABLE controllers RENAME COLUMN "DateCreated"     TO created_at;
ALTER TABLE controllers RENAME COLUMN "DateLastUpdated" TO updated_at;

-- 4.4 user_controller_assignments
ALTER TABLE user_controller_assignments RENAME COLUMN "Id"             TO id;
ALTER TABLE user_controller_assignments RENAME COLUMN "UserId"         TO user_id;
ALTER TABLE user_controller_assignments RENAME COLUMN "UnitId"         TO unit_id;
ALTER TABLE user_controller_assignments RENAME COLUMN "Permission"     TO permission;
ALTER TABLE user_controller_assignments RENAME COLUMN "SendSmsAlarm"   TO send_sms_alarm;
ALTER TABLE user_controller_assignments RENAME COLUMN "SendEmailAlarm" TO send_email_alarm;
ALTER TABLE user_controller_assignments RENAME COLUMN "CreatedAt"      TO created_at;
ALTER TABLE user_controller_assignments RENAME COLUMN "UpdatedAt"      TO updated_at;

-- 4.5 user_identities
ALTER TABLE user_identities RENAME COLUMN "Id"                    TO id;
ALTER TABLE user_identities RENAME COLUMN "UserId"                TO user_id;
ALTER TABLE user_identities RENAME COLUMN "Provider"              TO provider;
ALTER TABLE user_identities RENAME COLUMN "ProviderUserId"        TO provider_user_id;
ALTER TABLE user_identities RENAME COLUMN "ProviderEmail"         TO provider_email;
ALTER TABLE user_identities RENAME COLUMN "ProviderEmailVerified" TO provider_email_verified;
ALTER TABLE user_identities RENAME COLUMN "FriendlyName"          TO friendly_name;
ALTER TABLE user_identities RENAME COLUMN "IdentityData"          TO identity_data;
ALTER TABLE user_identities RENAME COLUMN "LinkedAt"              TO linked_at;
ALTER TABLE user_identities RENAME COLUMN "LastUsedAt"            TO last_used_at;

-- 4.6 user_mfa_factors
ALTER TABLE user_mfa_factors RENAME COLUMN "Id"              TO id;
ALTER TABLE user_mfa_factors RENAME COLUMN "UserId"          TO user_id;
ALTER TABLE user_mfa_factors RENAME COLUMN "FactorType"      TO factor_type;
ALTER TABLE user_mfa_factors RENAME COLUMN "IsEnabled"       TO enabled;
ALTER TABLE user_mfa_factors RENAME COLUMN "IsVerified"      TO verified;
ALTER TABLE user_mfa_factors RENAME COLUMN "IsPrimary"       TO is_primary;   -- primary 是关键字
ALTER TABLE user_mfa_factors RENAME COLUMN "SecretEncrypted" TO secret_encrypted;
ALTER TABLE user_mfa_factors RENAME COLUMN "CreatedAt"       TO created_at;
ALTER TABLE user_mfa_factors RENAME COLUMN "UpdatedAt"       TO updated_at;


-- ── 5. 约束改名 ─────────────────────────────────────────────────────────────
--
-- 统一到 PG 默认命名：<table>_pkey / <table>_<cols>_key / <table>_<cols>_fkey。
-- 之后建表都不显式命名约束，就不会再漂移。
--
-- 这里不硬编码旧约束名 —— 旧库里的实际名字可能因为历史上的手工操作而与
-- initDatabase.js 不一致，硬编码一旦对不上整个事务就会回滚。改成从 pg_constraint
-- 里查出实际名字，按新的表名/列名算出目标名，已经正确的跳过。
--
-- 必须在第 4 节列改名之后执行 —— cols 取的是改名后的列名。

DO $$
DECLARE
  r      record;
  target text;
BEGIN
  FOR r IN
    SELECT c.conname,
           c.contype,
           t.relname AS tbl,
           (SELECT string_agg(a.attname, '_' ORDER BY k.ord)
              FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
              JOIN pg_attribute a
                ON a.attrelid = c.conrelid AND a.attnum = k.attnum) AS cols
      FROM pg_constraint c
      JOIN pg_class     t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
     WHERE n.nspname = 'public'
       AND c.contype IN ('p', 'u', 'f')
       AND t.relname IN ('users', 'companies', 'controllers',
                         'user_controller_assignments', 'user_identities',
                         'user_mfa_factors')
  LOOP
    target := CASE r.contype
                WHEN 'p' THEN r.tbl || '_pkey'
                WHEN 'u' THEN r.tbl || '_' || r.cols || '_key'
                ELSE          r.tbl || '_' || r.cols || '_fkey'
              END;

    IF r.conname IS DISTINCT FROM target THEN
      EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', r.tbl, r.conname, target);
      RAISE NOTICE 'constraint % -> %', r.conname, target;
    END IF;
  END LOOP;
END $$;


-- ── 5b. 修正 companies.manager_user_id 的删除规则 ───────────────────────────
--
-- 旧 schema 写的是 ON DELETE CASCADE：删掉一个用户会**连带删掉他管理的整家公司**，
-- 再经由 controllers.company_id 的 ON DELETE SET NULL，让那家公司的所有设备失去
-- 公司归属 —— 也就失去了全部 Manager 的可见性。几乎肯定是建表时的笔误。
--
-- 改为 SET NULL：经理账号没了，公司还在，只是暂时没有经理。
-- （新建库的 initDatabase.js 已经是 SET NULL，这里让存量库对齐。）

ALTER TABLE companies DROP CONSTRAINT companies_manager_user_id_fkey;
ALTER TABLE companies
  ADD CONSTRAINT companies_manager_user_id_fkey
  FOREIGN KEY (manager_user_id) REFERENCES users(user_id) ON DELETE SET NULL;


-- ── 6. 索引改名 ─────────────────────────────────────────────────────────────
-- 索引名是 initDatabase.js 里显式写死的，可以硬编码；用 IF EXISTS 兜底，
-- 万一某个环境少建了一个索引也不至于整个脚本回滚。

ALTER INDEX IF EXISTS idx_userinfo_company_id        RENAME TO users_company_id_idx;
ALTER INDEX IF EXISTS idx_userinfo_activated         RENAME TO users_activated_idx;

ALTER INDEX IF EXISTS idx_useridentity_user_id       RENAME TO user_identities_user_id_idx;
ALTER INDEX IF EXISTS idx_useridentity_provider      RENAME TO user_identities_provider_idx;
ALTER INDEX IF EXISTS idx_useridentity_user_provider RENAME TO user_identities_user_id_provider_idx;

ALTER INDEX IF EXISTS idx_usermfafactor_user_id      RENAME TO user_mfa_factors_user_id_idx;
ALTER INDEX IF EXISTS idx_usermfafactor_factor_type  RENAME TO user_mfa_factors_factor_type_idx;

ALTER INDEX IF EXISTS idx_controllerinfo_owner_id     RENAME TO controllers_owner_user_id_idx;
ALTER INDEX IF EXISTS idx_controllerinfo_company_id   RENAME TO controllers_company_id_idx;
ALTER INDEX IF EXISTS idx_controllerinfo_activated    RENAME TO controllers_activated_idx;
ALTER INDEX IF EXISTS idx_controllerinfo_date_created RENAME TO controllers_created_at_idx;

ALTER INDEX IF EXISTS idx_uca_user_id    RENAME TO user_controller_assignments_user_id_idx;
ALTER INDEX IF EXISTS idx_uca_unit_id    RENAME TO user_controller_assignments_unit_id_idx;
ALTER INDEX IF EXISTS idx_uca_permission RENAME TO user_controller_assignments_permission_idx;

-- SERIAL 背后的序列名也是按旧表名生成的
ALTER SEQUENCE IF EXISTS "CompanyInfo_CompanyId_seq"       RENAME TO companies_company_id_seq;
ALTER SEQUENCE IF EXISTS "UserControllerAssignment_Id_seq" RENAME TO user_controller_assignments_id_seq;


-- ── 7. 重建触发器函数与触发器 ───────────────────────────────────────────────
-- controllers.updated_at 改名后与其它表同名，不再需要单独的触发器函数。

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_set_updated_at
  BEFORE UPDATE ON users
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER companies_set_updated_at
  BEFORE UPDATE ON companies
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER controllers_set_updated_at
  BEFORE UPDATE ON controllers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER user_controller_assignments_set_updated_at
  BEFORE UPDATE ON user_controller_assignments
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER user_mfa_factors_set_updated_at
  BEFORE UPDATE ON user_mfa_factors
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ── 8. O1：users.user_id 的序列 ─────────────────────────────────────────────
--
-- 建序列但**不设为 DEFAULT**。双跑期由 SQL Server 分配 user_id，PG 侧必须
-- 显式给值 —— 漏给会直接 NOT NULL 报错，而不是静默分配一个撞号的 id。
-- PgWriteGateway 在 pg-only 模式下会显式调 nextval()。
--
-- 切换到 PG 权威（P6）之后再执行：
--   ALTER TABLE users ALTER COLUMN user_id SET DEFAULT nextval('users_user_id_seq');

CREATE SEQUENCE IF NOT EXISTS users_user_id_seq AS integer OWNED BY users.user_id;
SELECT setval('users_user_id_seq',
              COALESCE((SELECT MAX(user_id) FROM users), 0) + 1,
              false);


-- ── 9. O6：UCA 的 id 变成纯 PG 序列 ─────────────────────────────────────────
--
-- 迁移时 id 直填了 SQL Server 的 SeqKey，双跑期两边各自自增会撞号。
-- 今后 id 完全由 PG 管理，跨库对齐键改用自然键 (user_id, unit_id)。
-- 这里把序列推到当前最大值之后。

SELECT setval('user_controller_assignments_id_seq',
              COALESCE((SELECT MAX(id) FROM user_controller_assignments), 0) + 1,
              false);


-- ── 10. O7：sync_source 列 ──────────────────────────────────────────────────
--
-- 标记行的来源。sqltopg 的孤儿清理只删 sync_source = 'mssql' 的行，
-- 避开"Pro 已写 PG、SQL Server 事务还没提交"的时间窗。

ALTER TABLE users                       ADD COLUMN IF NOT EXISTS sync_source VARCHAR(10) NOT NULL DEFAULT 'mssql';
ALTER TABLE controllers                 ADD COLUMN IF NOT EXISTS sync_source VARCHAR(10) NOT NULL DEFAULT 'mssql';
ALTER TABLE user_controller_assignments ADD COLUMN IF NOT EXISTS sync_source VARCHAR(10) NOT NULL DEFAULT 'mssql';

ALTER TABLE users
  ADD CONSTRAINT users_sync_source_check CHECK (sync_source IN ('mssql', 'pro'));
ALTER TABLE controllers
  ADD CONSTRAINT controllers_sync_source_check CHECK (sync_source IN ('mssql', 'pro'));
ALTER TABLE user_controller_assignments
  ADD CONSTRAINT user_controller_assignments_sync_source_check CHECK (sync_source IN ('mssql', 'pro'));


-- ── 11. O4：roles 小写规范化 ────────────────────────────────────────────────
-- 建表 DEFAULT 写的是 'Operator'，迁移脚本写的是 'operator'，invite 分支写的是
-- 'Operator' —— 大小写混用会让权限判定漏命中。统一成小写并去重。

UPDATE users
   SET roles = ARRAY(SELECT DISTINCT lower(r) FROM unnest(roles) AS r)
 WHERE EXISTS (SELECT 1 FROM unnest(roles) AS r WHERE r <> lower(r));

ALTER TABLE users
  ALTER COLUMN roles SET DEFAULT ARRAY['operator']::varchar[];


-- ── 12. O12：补索引 ─────────────────────────────────────────────────────────

-- 代码里到处 email.toLowerCase()，但旧的唯一约束是大小写敏感的
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_idx ON users (lower(email));

-- Manager 路径判定 (company_id = ANY(managed_company_ids)) 每次列表查询都要跑
CREATE INDEX IF NOT EXISTS users_managed_company_ids_idx ON users USING GIN (managed_company_ids);
CREATE INDEX IF NOT EXISTS users_roles_idx               ON users USING GIN (roles);


-- ── 13. permission 取值约束 ─────────────────────────────────────────────────
-- 防止把老的 AccessLevel 值（1 / 2）直接写进来。
-- 0 = READONLY, 50 = NORMAL, 100 = OWNER

ALTER TABLE user_controller_assignments
  ADD CONSTRAINT user_controller_assignments_permission_check
  CHECK (permission IN (0, 50, 100));


-- ── 14. 重建视图 ────────────────────────────────────────────────────────────

CREATE VIEW controller_user_permissions AS
SELECT a.id,
       a.user_id,
       u.name           AS user_name,
       u.email          AS user_email,
       a.unit_id,
       c.system_id,
       c.sim_card_no,
       a.permission,
       a.send_sms_alarm,
       a.send_email_alarm,
       a.created_at,
       a.updated_at
  FROM user_controller_assignments a
  JOIN users       u ON u.user_id = a.user_id
  JOIN controllers c ON c.unit_id = a.unit_id;

-- 旧名 "UserControllers" 看不出它带了 activated 过滤，改名为 active_ 前缀
CREATE VIEW active_user_controllers AS
SELECT u.user_id,
       u.name       AS user_name,
       u.email      AS user_email,
       c.unit_id,
       c.system_id,
       c.sim_card_no,
       c.activated  AS controller_activated,
       a.permission,
       a.created_at AS assigned_at
  FROM users u
  JOIN user_controller_assignments a ON a.user_id = u.user_id
  JOIN controllers                 c ON c.unit_id = a.unit_id
 WHERE u.activated = true
   AND c.activated = true;

COMMIT;

-- ── 15. 收尾（事务外）────────────────────────────────────────────────────────
ANALYZE users;
ANALYZE companies;
ANALYZE controllers;
ANALYZE user_controller_assignments;
