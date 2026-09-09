-- =============================================================================
-- 001_rename_to_snake_case_rollback.sql
--
-- 把 001 的改名回滚到 PascalCase。用于改名后发现应用侧还没准备好、需要把旧版
-- aquareporter-api / sqltopg 放回去的场景。
--
-- ⚠️ 不可逆的部分（回滚脚本无法还原，但都不影响旧代码运行）：
--    · 第 11 节把 roles 全部小写化了 —— 原始大小写丢失。
--      旧代码里 'Operator' / 'operator' 本来就混用，小写化后旧代码依然能跑，
--      只是权限判定比原来更准。不还原。
--    · 第 9 节把 UCA 的序列推到了 MAX(id)+1 —— 序列值只增不减，不还原。
--    · 新增的 sync_source 列在这里会被删掉，旧代码不认识它，删掉是对的。
--
-- 执行：psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f 001_rename_to_snake_case_rollback.sql
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- ── 1. 删掉新建的对象 ───────────────────────────────────────────────────────
DROP VIEW  IF EXISTS active_user_controllers      CASCADE;
DROP VIEW  IF EXISTS controller_user_permissions  CASCADE;

DROP TRIGGER IF EXISTS users_set_updated_at                       ON users;
DROP TRIGGER IF EXISTS companies_set_updated_at                   ON companies;
DROP TRIGGER IF EXISTS controllers_set_updated_at                 ON controllers;
DROP TRIGGER IF EXISTS user_controller_assignments_set_updated_at ON user_controller_assignments;
DROP TRIGGER IF EXISTS user_mfa_factors_set_updated_at            ON user_mfa_factors;
DROP FUNCTION IF EXISTS set_updated_at() CASCADE;

DROP INDEX    IF EXISTS users_email_lower_idx;
DROP INDEX    IF EXISTS users_managed_company_ids_idx;
DROP INDEX    IF EXISTS users_roles_idx;
DROP SEQUENCE IF EXISTS users_user_id_seq;

ALTER TABLE user_controller_assignments
  DROP CONSTRAINT IF EXISTS user_controller_assignments_permission_check;

ALTER TABLE users                       DROP CONSTRAINT IF EXISTS users_sync_source_check;
ALTER TABLE controllers                 DROP CONSTRAINT IF EXISTS controllers_sync_source_check;
ALTER TABLE user_controller_assignments DROP CONSTRAINT IF EXISTS user_controller_assignments_sync_source_check;

ALTER TABLE users                       DROP COLUMN IF EXISTS sync_source;
ALTER TABLE controllers                 DROP COLUMN IF EXISTS sync_source;
ALTER TABLE user_controller_assignments DROP COLUMN IF EXISTS sync_source;

ALTER TABLE users ALTER COLUMN roles SET DEFAULT ARRAY['Operator']::varchar[];

-- 5b 的反向：把 companies.manager_user_id 的删除规则改回 ON DELETE CASCADE。
-- （CASCADE 是危险的 —— 删用户会连带删公司 —— 但回滚就该回到原样。）
ALTER TABLE companies DROP CONSTRAINT companies_manager_user_id_fkey;
ALTER TABLE companies
  ADD CONSTRAINT companies_manager_user_id_fkey
  FOREIGN KEY (manager_user_id) REFERENCES users(user_id) ON DELETE CASCADE;


-- ── 2. 序列 / 索引 / 约束改回 ───────────────────────────────────────────────
ALTER SEQUENCE companies_company_id_seq               RENAME TO "CompanyInfo_CompanyId_seq";
ALTER SEQUENCE user_controller_assignments_id_seq     RENAME TO "UserControllerAssignment_Id_seq";

ALTER INDEX users_company_id_idx                      RENAME TO idx_userinfo_company_id;
ALTER INDEX users_activated_idx                       RENAME TO idx_userinfo_activated;
ALTER INDEX user_identities_user_id_idx               RENAME TO idx_useridentity_user_id;
ALTER INDEX user_identities_provider_idx              RENAME TO idx_useridentity_provider;
ALTER INDEX user_identities_user_id_provider_idx      RENAME TO idx_useridentity_user_provider;
ALTER INDEX user_mfa_factors_user_id_idx              RENAME TO idx_usermfafactor_user_id;
ALTER INDEX user_mfa_factors_factor_type_idx          RENAME TO idx_usermfafactor_factor_type;
ALTER INDEX controllers_owner_user_id_idx             RENAME TO idx_controllerinfo_owner_id;
ALTER INDEX controllers_company_id_idx                RENAME TO idx_controllerinfo_company_id;
ALTER INDEX controllers_activated_idx                 RENAME TO idx_controllerinfo_activated;
ALTER INDEX controllers_created_at_idx                RENAME TO idx_controllerinfo_date_created;
ALTER INDEX user_controller_assignments_user_id_idx   RENAME TO idx_uca_user_id;
ALTER INDEX user_controller_assignments_unit_id_idx   RENAME TO idx_uca_unit_id;
ALTER INDEX user_controller_assignments_permission_idx RENAME TO idx_uca_permission;

ALTER TABLE users RENAME CONSTRAINT users_email_key TO "UserInfo_Email_unique";
ALTER TABLE user_controller_assignments
  RENAME CONSTRAINT user_controller_assignments_user_id_unit_id_key TO "UCA_UserId_UnitId_unique";
ALTER TABLE user_identities
  RENAME CONSTRAINT user_identities_provider_provider_user_id_key
    TO "UserIdentity_Provider_ProviderUserId_unique";

ALTER TABLE users                       RENAME CONSTRAINT users_pkey                       TO "UserInfo_pkey";
ALTER TABLE companies                   RENAME CONSTRAINT companies_pkey                   TO "CompanyInfo_pkey";
ALTER TABLE controllers                 RENAME CONSTRAINT controllers_pkey                 TO "ControllerInfo_pkey";
ALTER TABLE user_controller_assignments RENAME CONSTRAINT user_controller_assignments_pkey TO "UserControllerAssignment_pkey";
ALTER TABLE user_identities             RENAME CONSTRAINT user_identities_pkey             TO "UserIdentity_pkey";
ALTER TABLE user_mfa_factors            RENAME CONSTRAINT user_mfa_factors_pkey            TO "UserMfaFactor_pkey";

ALTER TABLE companies                   RENAME CONSTRAINT companies_manager_user_id_fkey            TO "CompanyInfo_ManagerId_fkey";
ALTER TABLE controllers                 RENAME CONSTRAINT controllers_owner_user_id_fkey            TO "ControllerInfo_OwnerId_fkey";
ALTER TABLE controllers                 RENAME CONSTRAINT controllers_company_id_fkey               TO "ControllerInfo_CompanyId_fkey";
ALTER TABLE user_controller_assignments RENAME CONSTRAINT user_controller_assignments_user_id_fkey  TO "UserControllerAssignment_UserId_fkey";
ALTER TABLE user_controller_assignments RENAME CONSTRAINT user_controller_assignments_unit_id_fkey  TO "UserControllerAssignment_UnitId_fkey";
ALTER TABLE user_identities             RENAME CONSTRAINT user_identities_user_id_fkey              TO "UserIdentity_UserId_fkey";
ALTER TABLE user_mfa_factors            RENAME CONSTRAINT user_mfa_factors_user_id_fkey             TO "UserMfaFactor_UserId_fkey";


-- ── 3. 列改回 ───────────────────────────────────────────────────────────────
ALTER TABLE users RENAME COLUMN user_id             TO "UserId";
ALTER TABLE users RENAME COLUMN company_id          TO "CompanyId";
ALTER TABLE users RENAME COLUMN email               TO "Email";
ALTER TABLE users RENAME COLUMN email_verified      TO "EmailVerified";
ALTER TABLE users RENAME COLUMN mobile              TO "Mobile";
ALTER TABLE users RENAME COLUMN mobile_verified     TO "MobileVerified";
ALTER TABLE users RENAME COLUMN name                TO "Name";
ALTER TABLE users RENAME COLUMN password_hash       TO "PasswordHash";
ALTER TABLE users RENAME COLUMN avatar_url          TO "Avatar";
ALTER TABLE users RENAME COLUMN roles               TO "Roles";
ALTER TABLE users RENAME COLUMN managed_company_ids TO "ManagedCompanyIds";
ALTER TABLE users RENAME COLUMN activated           TO "Activated";
ALTER TABLE users RENAME COLUMN mfa_enabled         TO "MfaEnabled";
ALTER TABLE users RENAME COLUMN linked_providers    TO "LinkedProviders";
ALTER TABLE users RENAME COLUMN last_login_at       TO "LastLoginTime";
ALTER TABLE users RENAME COLUMN created_at          TO "CreatedAt";
ALTER TABLE users RENAME COLUMN updated_at          TO "UpdatedAt";

ALTER TABLE companies RENAME COLUMN company_id      TO "CompanyId";
ALTER TABLE companies RENAME COLUMN name            TO "CompanyName";
ALTER TABLE companies RENAME COLUMN manager_user_id TO "ManagerId";
ALTER TABLE companies RENAME COLUMN jw_code         TO "JwCode";
ALTER TABLE companies RENAME COLUMN description     TO "Desc";
ALTER TABLE companies RENAME COLUMN phone           TO "Phone";
ALTER TABLE companies RENAME COLUMN email           TO "Email";
ALTER TABLE companies RENAME COLUMN logo_url        TO "CompanyLogo";
ALTER TABLE companies RENAME COLUMN activated       TO "Activated";
ALTER TABLE companies RENAME COLUMN created_at      TO "CreatedAt";
ALTER TABLE companies RENAME COLUMN updated_at      TO "UpdatedAt";

ALTER TABLE controllers RENAME COLUMN unit_id           TO "UnitId";
ALTER TABLE controllers RENAME COLUMN serial_no         TO "SerialNo";
ALTER TABLE controllers RENAME COLUMN sim_card_no       TO "SimCardNo";
ALTER TABLE controllers RENAME COLUMN owner_user_id     TO "OwnerId";
ALTER TABLE controllers RENAME COLUMN company_id        TO "CompanyId";
ALTER TABLE controllers RENAME COLUMN system_id         TO "SystemId";
ALTER TABLE controllers RENAME COLUMN firmware_version  TO "FirmwareVersion";
ALTER TABLE controllers RENAME COLUMN model_type        TO "ModelType";
ALTER TABLE controllers RENAME COLUMN timezone_id       TO "TimezoneId";
ALTER TABLE controllers RENAME COLUMN production_status TO "Status";
ALTER TABLE controllers RENAME COLUMN activated         TO "Activated";
ALTER TABLE controllers RENAME COLUMN site_location     TO "Creator";
ALTER TABLE controllers RENAME COLUMN tags              TO "Tags";
ALTER TABLE controllers RENAME COLUMN billing_email     TO "BillingEmail";
ALTER TABLE controllers RENAME COLUMN bar_code          TO "BarCode";
ALTER TABLE controllers RENAME COLUMN po_number         TO "PoNumber";
ALTER TABLE controllers RENAME COLUMN spare_parts       TO "SpareParts";
ALTER TABLE controllers RENAME COLUMN notes             TO "Notes";
ALTER TABLE controllers RENAME COLUMN created_at        TO "DateCreated";
ALTER TABLE controllers RENAME COLUMN updated_at        TO "DateLastUpdated";

ALTER TABLE user_controller_assignments RENAME COLUMN id               TO "Id";
ALTER TABLE user_controller_assignments RENAME COLUMN user_id          TO "UserId";
ALTER TABLE user_controller_assignments RENAME COLUMN unit_id          TO "UnitId";
ALTER TABLE user_controller_assignments RENAME COLUMN permission       TO "Permission";
ALTER TABLE user_controller_assignments RENAME COLUMN send_sms_alarm   TO "SendSmsAlarm";
ALTER TABLE user_controller_assignments RENAME COLUMN send_email_alarm TO "SendEmailAlarm";
ALTER TABLE user_controller_assignments RENAME COLUMN created_at       TO "CreatedAt";
ALTER TABLE user_controller_assignments RENAME COLUMN updated_at       TO "UpdatedAt";

ALTER TABLE user_identities RENAME COLUMN id                      TO "Id";
ALTER TABLE user_identities RENAME COLUMN user_id                 TO "UserId";
ALTER TABLE user_identities RENAME COLUMN provider                TO "Provider";
ALTER TABLE user_identities RENAME COLUMN provider_user_id        TO "ProviderUserId";
ALTER TABLE user_identities RENAME COLUMN provider_email          TO "ProviderEmail";
ALTER TABLE user_identities RENAME COLUMN provider_email_verified TO "ProviderEmailVerified";
ALTER TABLE user_identities RENAME COLUMN friendly_name           TO "FriendlyName";
ALTER TABLE user_identities RENAME COLUMN identity_data           TO "IdentityData";
ALTER TABLE user_identities RENAME COLUMN linked_at               TO "LinkedAt";
ALTER TABLE user_identities RENAME COLUMN last_used_at            TO "LastUsedAt";

ALTER TABLE user_mfa_factors RENAME COLUMN id               TO "Id";
ALTER TABLE user_mfa_factors RENAME COLUMN user_id          TO "UserId";
ALTER TABLE user_mfa_factors RENAME COLUMN factor_type      TO "FactorType";
ALTER TABLE user_mfa_factors RENAME COLUMN enabled          TO "IsEnabled";
ALTER TABLE user_mfa_factors RENAME COLUMN verified         TO "IsVerified";
ALTER TABLE user_mfa_factors RENAME COLUMN is_primary       TO "IsPrimary";
ALTER TABLE user_mfa_factors RENAME COLUMN secret_encrypted TO "SecretEncrypted";
ALTER TABLE user_mfa_factors RENAME COLUMN created_at       TO "CreatedAt";
ALTER TABLE user_mfa_factors RENAME COLUMN updated_at       TO "UpdatedAt";


-- ── 4. 表改回 ───────────────────────────────────────────────────────────────
ALTER TABLE users                       RENAME TO "UserInfo";
ALTER TABLE companies                   RENAME TO "CompanyInfo";
ALTER TABLE controllers                 RENAME TO "ControllerInfo";
ALTER TABLE user_controller_assignments RENAME TO "UserControllerAssignment";
ALTER TABLE user_identities             RENAME TO "UserIdentity";
ALTER TABLE user_mfa_factors            RENAME TO "UserMfaFactor";


-- ── 5. 重建旧触发器函数与视图 ───────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW."UpdatedAt" = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_controllers_date_last_updated()
RETURNS TRIGGER AS $$
BEGIN
  NEW."DateLastUpdated" = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_userinfo_updated_at       BEFORE UPDATE ON "UserInfo"
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trg_companyinfo_updated_at    BEFORE UPDATE ON "CompanyInfo"
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trg_usermfafactor_updated_at  BEFORE UPDATE ON "UserMfaFactor"
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trg_uca_updated_at            BEFORE UPDATE ON "UserControllerAssignment"
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER trg_controllerinfo_updated_at BEFORE UPDATE ON "ControllerInfo"
  FOR EACH ROW EXECUTE FUNCTION update_controllers_date_last_updated();

CREATE VIEW "UserFullInfo" AS
SELECT u."UserId", u."CompanyId", u."Roles", u."ManagedCompanyIds", u."Mobile",
       u."MobileVerified", u."Email", u."EmailVerified", u."Name", u."Avatar",
       u."Activated", u."MfaEnabled", u."LinkedProviders", u."LastLoginTime",
       u."CreatedAt", u."UpdatedAt"
  FROM "UserInfo" u;

CREATE VIEW "ControllerUserPermissions" AS
SELECT a."Id", a."UserId", u."Name" AS "UserName", u."Email" AS "UserEmail",
       a."UnitId", c."SystemId", c."SimCardNo", a."Permission",
       a."SendSmsAlarm", a."SendEmailAlarm", a."CreatedAt", a."UpdatedAt"
  FROM "UserControllerAssignment" a
  JOIN "UserInfo"       u ON a."UserId" = u."UserId"
  JOIN "ControllerInfo" c ON a."UnitId" = c."UnitId";

CREATE VIEW "UserControllers" AS
SELECT u."UserId", u."Name" AS "UserName", u."Email" AS "UserEmail",
       c."UnitId", c."SystemId", c."SimCardNo",
       c."Activated" AS "ControllerActivated", a."Permission",
       a."CreatedAt" AS "AssignedAt"
  FROM "UserInfo" u
  JOIN "UserControllerAssignment" a ON u."UserId" = a."UserId"
  JOIN "ControllerInfo"           c ON a."UnitId" = c."UnitId"
 WHERE u."Activated" = true AND c."Activated" = true;

COMMIT;
