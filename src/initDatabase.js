import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config();

const { Client } = pg;

async function initDatabase() {
  const client = new Client({
    connectionString: process.env.POSTGRESQL_URL,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  console.log('Connected to PostgreSQL');

  try {
    await client.query('BEGIN');

    // ── 1. Drop views ─────────────────────────────────────────────────────────
    console.log('Dropping views...');
    await client.query(`DROP VIEW IF EXISTS "UserControllers"            CASCADE;`);
    await client.query(`DROP VIEW IF EXISTS "ControllerUserPermissions"  CASCADE;`);
    await client.query(`DROP VIEW IF EXISTS "UserFullInfo"               CASCADE;`);

    // ── 2. Drop tables (children first) ──────────────────────────────────────
    console.log('Dropping tables...');
    await client.query(`DROP TABLE IF EXISTS "UserIdentity"              CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "UserMfaFactor"             CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "UserControllerAssignment"  CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "ControllerInfo"            CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "UserProfiles"              CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "CompanyInfo"               CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS "UserInfo"                  CASCADE;`);

    // ── 3. Drop enums & functions ─────────────────────────────────────────────
    // permission_level ENUM was removed — Permission is now SMALLINT.
    // The DROP below keeps old databases clean during re-init.
    await client.query(`DROP TYPE     IF EXISTS permission_level                       CASCADE;`);
    await client.query(`DROP FUNCTION IF EXISTS update_updated_at_column()             CASCADE;`);
    await client.query(`DROP FUNCTION IF EXISTS update_controllers_date_last_updated() CASCADE;`);

    console.log('All objects dropped.');

    // ── 4. Shared trigger function ────────────────────────────────────────────
    await client.query(`
      CREATE OR REPLACE FUNCTION update_updated_at_column()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW."UpdatedAt" = NOW();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // ── 5. UserInfo ───────────────────────────────────────────────────────────
    console.log('Creating "UserInfo"...');
    await client.query(`
      CREATE TABLE "UserInfo" (
        "UserId"             INTEGER PRIMARY KEY,
        "CompanyId"          INTEGER NOT NULL,
        "Email"              VARCHAR(100) NOT NULL,
        "EmailVerified"      BOOLEAN      DEFAULT false,
        "Mobile"             VARCHAR(50)  DEFAULT '0000000000',
        "MobileVerified"     BOOLEAN      DEFAULT false,
        "Name"               VARCHAR(100) NOT NULL,
        "PasswordHash"       VARCHAR(255) NOT NULL,
        "Avatar"             VARCHAR(255),
        "Roles"              VARCHAR[]    DEFAULT ARRAY['Operator']::VARCHAR[],
        "ManagedCompanyIds"  INTEGER[],
        "Activated"          BOOLEAN      NOT NULL DEFAULT true,
        "MfaEnabled"         BOOLEAN      NOT NULL DEFAULT false,
        "LinkedProviders"    VARCHAR[]    NOT NULL DEFAULT '{}',
        "LastLoginTime"      TIMESTAMP WITH TIME ZONE,
        "CreatedAt"          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "UpdatedAt"          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        CONSTRAINT "UserInfo_Email_unique" UNIQUE ("Email")
      );
    `);
    await client.query(`CREATE INDEX idx_userinfo_company_id ON "UserInfo"("CompanyId");`);
    await client.query(`CREATE INDEX idx_userinfo_activated   ON "UserInfo"("Activated");`);
    await client.query(`
      CREATE TRIGGER trg_userinfo_updated_at
        BEFORE UPDATE ON "UserInfo"
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    `);

    // ── 6. CompanyInfo ────────────────────────────────────────────────────────
    console.log('Creating "CompanyInfo"...');
    await client.query(`
      CREATE TABLE "CompanyInfo" (
        "CompanyId"   SERIAL PRIMARY KEY,
        "CompanyName" VARCHAR(200) NOT NULL UNIQUE,
        "ManagerId"   INTEGER REFERENCES "UserInfo"("UserId") ON DELETE CASCADE,
        "JwCode"      VARCHAR(20),
        "Desc"        VARCHAR(200),
        "Phone"       VARCHAR(50)  DEFAULT '0000000000',
        "Email"       VARCHAR(100),
        "CompanyLogo" VARCHAR(500) DEFAULT 'YourLogoGoesToHere.JPG',
        "Activated"   BOOLEAN NOT NULL DEFAULT true,
        "CreatedAt"   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "UpdatedAt"   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);
    await client.query(`
      CREATE TRIGGER trg_companyinfo_updated_at
        BEFORE UPDATE ON "CompanyInfo"
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    `);

    // ── 7. UserIdentity ───────────────────────────────────────────────────────
    console.log('Creating "UserIdentity"...');
    await client.query(`
      CREATE TABLE "UserIdentity" (
        "Id"                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
        "UserId"               INTEGER      NOT NULL REFERENCES "UserInfo"("UserId") ON DELETE CASCADE,
        "Provider"             VARCHAR(50)  NOT NULL,
        "ProviderUserId"       VARCHAR(255) NOT NULL,
        "ProviderEmail"        VARCHAR(100),
        "ProviderEmailVerified" BOOLEAN,
        "LinkedAt"             TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "LastUsedAt"           TIMESTAMP WITH TIME ZONE,
        CONSTRAINT "UserIdentity_Provider_ProviderUserId_unique" UNIQUE ("Provider", "ProviderUserId")
      );
    `);
    await client.query(`CREATE INDEX idx_useridentity_user_id  ON "UserIdentity"("UserId");`);
    await client.query(`CREATE INDEX idx_useridentity_provider ON "UserIdentity"("Provider");`);

    // ── 8. UserMfaFactor ──────────────────────────────────────────────────────
    console.log('Creating "UserMfaFactor"...');
    await client.query(`
      CREATE TABLE "UserMfaFactor" (
        "Id"               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
        "UserId"           INTEGER      NOT NULL REFERENCES "UserInfo"("UserId") ON DELETE CASCADE,
        "FactorType"       VARCHAR(50)  NOT NULL,
        "IsEnabled"        BOOLEAN      NOT NULL DEFAULT true,
        "IsVerified"       BOOLEAN      NOT NULL DEFAULT false,
        "IsPrimary"        BOOLEAN      NOT NULL DEFAULT false,
        "SecretEncrypted"  TEXT,
        "CreatedAt"        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "UpdatedAt"        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      );
    `);
    await client.query(`CREATE INDEX idx_usermfafactor_user_id     ON "UserMfaFactor"("UserId");`);
    await client.query(`CREATE INDEX idx_usermfafactor_factor_type ON "UserMfaFactor"("FactorType");`);
    await client.query(`
      CREATE TRIGGER trg_usermfafactor_updated_at
        BEFORE UPDATE ON "UserMfaFactor"
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    `);

    // ── 9. ControllerInfo ─────────────────────────────────────────────────────
    console.log('Creating "ControllerInfo"...');
    await client.query(`
      CREATE OR REPLACE FUNCTION update_controllers_date_last_updated()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW."DateLastUpdated" = NOW();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);
    await client.query(`
      CREATE TABLE "ControllerInfo" (
        "UnitId"          INTEGER      PRIMARY KEY,
        "SerialNo"        INTEGER,
        "SimCardNo"       VARCHAR(50)  NOT NULL,
        "OwnerId"         INTEGER      REFERENCES "UserInfo"("UserId")    ON DELETE SET NULL,
        "CompanyId"       INTEGER      REFERENCES "CompanyInfo"("CompanyId") ON DELETE SET NULL,
        "SystemId"        VARCHAR(100) NOT NULL,
        "FirmwareVersion" VARCHAR(50),
        "ModelType"       VARCHAR(50),
        "TimezoneId"      VARCHAR(50),
        "Status"          INTEGER, -- 生产状态 3:生产中，4:已完成
        "Activated"       BOOLEAN      NOT NULL DEFAULT false,
        "DateCreated"     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "DateLastUpdated" TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "Creator"         VARCHAR(255),
        "Tags"            TEXT[],
        "BillingEmail"    VARCHAR(100),
        "BarCode"         VARCHAR(50),
        "PoNumber"        VARCHAR(50),
        "SpareParts"      VARCHAR(200),
        "Notes"           TEXT
      );
    `);
    await client.query(`CREATE INDEX idx_controllerinfo_owner_id     ON "ControllerInfo"("OwnerId");`);
    await client.query(`CREATE INDEX idx_controllerinfo_company_id   ON "ControllerInfo"("CompanyId");`);
    await client.query(`CREATE INDEX idx_controllerinfo_activated    ON "ControllerInfo"("Activated");`);
    await client.query(`CREATE INDEX idx_controllerinfo_date_created ON "ControllerInfo"("DateCreated");`);
    await client.query(`
      CREATE TRIGGER trg_controllerinfo_updated_at
        BEFORE UPDATE ON "ControllerInfo"
        FOR EACH ROW EXECUTE FUNCTION update_controllers_date_last_updated();
    `);

    // ── 10. UserControllerAssignment ──────────────────────────────────────────
    //
    //   Permission (SMALLINT) — numeric rank, higher = more privilege.
    //   Gaps are intentional so new tiers can be inserted later (e.g. VIEWER=20,
    //   EDITOR=70) without any schema migration.
    //
    //     0   READONLY  — read-only access
    //    50   NORMAL    — default working access
    //   100   OWNER     — full control of the controller
    //
    //   Application code should reference these constants by name rather than
    //   hard-coding the numbers at call sites.
    //
    console.log('Creating "UserControllerAssignment"...');
    await client.query(`
      CREATE TABLE "UserControllerAssignment" (
        "Id"              SERIAL    PRIMARY KEY,
        "UserId"          INTEGER   NOT NULL REFERENCES "UserInfo"("UserId")       ON DELETE CASCADE,
        "UnitId"          INTEGER   NOT NULL REFERENCES "ControllerInfo"("UnitId") ON DELETE CASCADE,
        "Permission"      SMALLINT  NOT NULL DEFAULT 50,
        "SendSmsAlarm"    BOOLEAN   DEFAULT false,
        "SendEmailAlarm"  BOOLEAN   DEFAULT false,
        "CreatedAt"       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        "UpdatedAt"       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        CONSTRAINT "UCA_UserId_UnitId_unique" UNIQUE ("UserId", "UnitId")
      );
    `);
    await client.query(`CREATE INDEX idx_uca_user_id          ON "UserControllerAssignment"("UserId");`);
    await client.query(`CREATE INDEX idx_uca_unit_id          ON "UserControllerAssignment"("UnitId");`);
    await client.query(`CREATE INDEX idx_uca_permission ON "UserControllerAssignment"("Permission");`);
    await client.query(`
      CREATE TRIGGER trg_uca_updated_at
        BEFORE UPDATE ON "UserControllerAssignment"
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    `);

    // ── 11. Views ─────────────────────────────────────────────────────────────
    console.log('Creating views...');
    await client.query(`
      CREATE VIEW "UserFullInfo" AS
      SELECT
        u."UserId",
        u."CompanyId",
        u."Roles",
        u."ManagedCompanyIds",
        u."Mobile",
        u."MobileVerified",
        u."Email",
        u."EmailVerified",
        u."Name",
        u."Avatar",
        u."Activated",
        u."MfaEnabled",
        u."LinkedProviders",
        u."LastLoginTime",
        u."CreatedAt",
        u."UpdatedAt"
      FROM "UserInfo" u;
    `);
    await client.query(`
      CREATE VIEW "ControllerUserPermissions" AS
      SELECT
        a."Id",
        a."UserId",
        u."Name"            AS "UserName",
        u."Email"           AS "UserEmail",
        a."UnitId",
        c."SystemId",
        c."SimCardNo",
        a."Permission",
        a."SendSmsAlarm",
        a."SendEmailAlarm",
        a."CreatedAt",
        a."UpdatedAt"
      FROM "UserControllerAssignment" a
      JOIN "UserInfo"       u ON a."UserId" = u."UserId"
      JOIN "ControllerInfo" c ON a."UnitId" = c."UnitId";
    `);
    await client.query(`
      CREATE VIEW "UserControllers" AS
      SELECT
        u."UserId",
        u."Name"            AS "UserName",
        u."Email"           AS "UserEmail",
        c."UnitId",
        c."SystemId",
        c."SimCardNo",
        c."Activated"       AS "ControllerActivated",
        a."Permission",
        a."CreatedAt"       AS "AssignedAt"
      FROM "UserInfo" u
      JOIN "UserControllerAssignment" a ON u."UserId" = a."UserId"
      JOIN "ControllerInfo"           c ON a."UnitId" = c."UnitId"
      WHERE u."Activated" = true AND c."Activated" = true;
    `);

    await client.query('COMMIT');
    console.log('Database initialized successfully.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Initialization failed, rolled back:', err.message);
    process.exit(1);
  } finally {
    await client.end();
  }
}

initDatabase();
