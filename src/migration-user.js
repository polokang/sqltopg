/**
 * migration-user.js
 *
 * Migrate UserInfo: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : AquariusEmailDB.dbo.UserInfomation (all rows)
 * Target : PostgreSQL public."UserInfo"
 *
 * Column mapping:
 *   UserId               → "UserId"
 *   CompanyId            → "CompanyId"
 *   Email                → "Email"
 *   Activated=1          → "EmailVerified" = true, else false
 *   Mobile               → "Mobile"
 *   IsPhoneVerified      → "MobileVerified"
 *   UserName             → "Name"
 *   PasswordHash         → "PasswordHash"
 *   Logo                 → "Avatar"
 *   Roles (derived):
 *     UserTypeId = 22    → ['super']
 *     Production = 1     → ['manager'],  ManagedCompanyIds = [CompanyId]
 *     otherwise          → ['operator'], ManagedCompanyIds = []
 *   Activated            → "Activated"
 *   RequiresTwoFactorAuth → "MfaEnabled"
 *   LastLoginTime        → "LastLoginTime"
 *   DateCreated          → "CreatedAt"
 *   LastUpdateDate       → "UpdatedAt"
 *
 * Run:
 *   node src/migration-user.js
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { startProgress } from './progress.js';

dotenv.config();

// ─── PostgreSQL connection ────────────────────────────────────────────────────

const pgPool = new Pool({
  connectionString: process.env.POSTGRESQL_URL,
  ssl: { rejectUnauthorized: false },
});

// ─── SQL Server connection ────────────────────────────────────────────────────

function parseSqlServerUrl(url = '') {
  const clean = url.replace(/^["']?sqlserver:\/\/["']?/i, '').trim();
  const cfg = { options: { encrypt: true, trustServerCertificate: true } };

  const hostMatch = clean.match(/^([^;:]+):?(\d+)?/);
  if (hostMatch) {
    cfg.server = hostMatch[1];
    if (hostMatch[2]) cfg.port = parseInt(hostMatch[2], 10);
  }

  for (const part of clean.split(';').slice(1)) {
    const eq = part.indexOf('=');
    if (eq < 1) continue;
    const k = part.slice(0, eq).trim().toLowerCase();
    const v = part.slice(eq + 1).trim();
    if (k === 'database')                  cfg.database = v;
    else if (k === 'user')                 cfg.user = v;
    else if (k === 'password')             cfg.password = v;
    else if (k === 'trustservercertificate')
      cfg.options.trustServerCertificate = v === 'true';
  }

  return cfg;
}

const sqlConfig = parseSqlServerUrl(process.env.SQL_SERVER_URL);

// ─── Role derivation ──────────────────────────────────────────────────────────

function deriveRolesAndCompanies(row) {
  // UserTypeId=22 → super, takes absolute priority over everything else
  if (row.UserTypeId === 22) {
    return { roles: ['super'], managedCompanyIds: [] };
  }

  const roles = [];
  let managedCompanyIds = [];

  // Production=1 → manager + owner, manages own CompanyId
  if (row.Production === 1 || row.Production === true) {
    roles.push('manager', 'owner');
    managedCompanyIds = [row.CompanyId];
  }

  // UserTypeId=24 → at least manager + manages own CompanyId
  if (row.UserTypeId === 24 && !roles.includes('manager')) {
    roles.push('manager');
    managedCompanyIds = [row.CompanyId];
  }

  // Default fallback
  if (roles.length === 0) {
    roles.push('operator');
  }

  return { roles, managedCompanyIds };
}

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateUserInfo(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  const result = await sqlPool.request().query(`
    SELECT
      UserId,
      CompanyId,
      Email,
      Mobile,
      IsPhoneVerified,
      UserName,
      Name,
      PasswordHash,
      Logo,
      UserTypeId,
      Production,
      Activated,
      RequiresTwoFactorAuth,
      LastLoginTime,
      DateCreated,
      LastUpdateDate
    FROM AquariusEmailDB.dbo.UserInfomation
    WHERE Email IS NOT NULL
  `);

  const rows = result.recordset;
  console.log(`✓ Read ${rows.length} rows from UserInfomation`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  let inserted = 0;
  const progress = startProgress(rows.length, 'UserInfo     ');

  for (const row of rows) {
    const { roles, managedCompanyIds } = deriveRolesAndCompanies(row);

    await pgClient.query(
      `INSERT INTO "UserInfo" (
         "UserId",
         "CompanyId",
         "Email",
         "EmailVerified",
         "Mobile",
         "MobileVerified",
         "Name",
         "PasswordHash",
         "Avatar",
         "Roles",
         "ManagedCompanyIds",
         "Activated",
         "MfaEnabled",
         "LastLoginTime",
         "CreatedAt",
         "UpdatedAt"
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        row.UserId,
        row.CompanyId,
        row.Email,
        row.Activated === 1 || row.Activated === true,                          // EmailVerified
        row.Mobile || '0000000000',
        row.IsPhoneVerified === 1 || row.IsPhoneVerified === true,              // MobileVerified
        row.UserName || row.Name || row.Email,                                   // Name: UserName → Name → Email
        row.PasswordHash || '',
        row.Logo || null,                                                        // Avatar
        roles,
        managedCompanyIds,
        row.Activated === 1 || row.Activated === true,                          // Activated
        row.RequiresTwoFactorAuth === 1 || row.RequiresTwoFactorAuth === true,  // MfaEnabled
        row.LastLoginTime || null,
        row.DateCreated,
        row.LastUpdateDate,
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);
}

// ─── Restore CompanyInfo.ManagerId FK (if not yet restored) ──────────────────

async function restoreCompanyManagerFk(pgClient) {
  // The FK was dropped in migration-company.js to allow inserting raw UserId
  // values before UserInfo existed. Now that UserInfo is populated, restore it.
  await pgClient.query(`
    ALTER TABLE "CompanyInfo"
      DROP CONSTRAINT IF EXISTS "CompanyInfo_ManagerId_fkey";
  `);
  await pgClient.query(`
    ALTER TABLE "CompanyInfo"
      ADD CONSTRAINT "CompanyInfo_ManagerId_fkey"
        FOREIGN KEY ("ManagerId")
        REFERENCES "UserInfo" ("UserId")
        ON DELETE CASCADE;
  `);
  console.log('✓ Restored CompanyInfo_ManagerId_fkey → UserInfo("UserId")');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateUserInfo(pgClient);
    await pgClient.query('COMMIT');

    // Restore the FK that migration-company.js temporarily dropped
    await restoreCompanyManagerFk(pgClient);

    console.log('\n✓ migration-user completed.');
    console.log('Next: verify ManagerId references with:');
    console.log('  SELECT c."CompanyId", c."ManagerId"');
    console.log('  FROM "CompanyInfo" c');
    console.log('  LEFT JOIN "UserInfo" u ON c."ManagerId" = u."UserId"');
    console.log('  WHERE c."ManagerId" IS NOT NULL AND u."UserId" IS NULL;');
  } catch (err) {
    await pgClient.query('ROLLBACK').catch(() => {});
    console.error('\n✗ Migration failed — rolled back:', err.message);
    console.error(err.stack);
    process.exit(1);
  } finally {
    pgClient.release();
    await pgPool.end();
  }
}

run();
