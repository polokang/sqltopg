/**
 * sync-simple.js
 *
 * Ongoing sync: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG).
 *
 * Tables kept in sync (PascalCase target schema, see initDatabase.js):
 *   "UserInfo"                 ← UserInfomation
 *   "ControllerInfo"           ← ControllerInfo  (LEFT JOIN UserInfomation for CompanyId/BillingEmail)
 *   "UserControllerAssignment" ← UserControllerAssignment
 *
 * Strategy: fetch all rows from SQL Server, upsert into PG by primary key,
 * then delete PG rows whose PK disappeared from the source.
 * All three tables are synced inside a single transaction for atomicity.
 *
 * NOT synced here (one-off migrations owned by scripts):
 *   "CompanyInfo"     (see migration-company.js)
 *   "UserIdentity"    (identity-provider data, app-owned)
 *   "UserMfaFactor"   (MFA secrets, app-owned)
 *
 * Derivation rules are kept in lock-step with the one-off migration scripts:
 *   - Role derivation       → see migration-user.js
 *   - Suburb → Tags parsing → see migration-controller.js
 *   - AccessLevel → Permission mapping → see migration-assignment.js
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { fileURLToPath } from 'url';
import { resolve } from 'path';

dotenv.config();

// ─── PostgreSQL connection ────────────────────────────────────────────────────

const pgPool = new Pool({
  connectionString: process.env.POSTGRESQL_URL,
  ssl: { rejectUnauthorized: false },
});

// ─── SQL Server connection ────────────────────────────────────────────────────

function parseSqlServerUrl(url = '') {
  const clean = url.replace(/^["']?sqlserver:\/\/["']?/i, '').trim();
  const cfg = { options: { encrypt: true, trustServerCertificate: true, enableArithAbort: true } };

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
    if (k === 'database')                    cfg.database = v;
    else if (k === 'user')                   cfg.user = v;
    else if (k === 'password')               cfg.password = v;
    else if (k === 'trustservercertificate') cfg.options.trustServerCertificate = v === 'true';
  }

  cfg.pool = { max: 10, min: 0, idleTimeoutMillis: 30000 };
  return cfg;
}

const sqlConfig = parseSqlServerUrl(process.env.SQL_SERVER_URL || '');

// ─── Shared helpers ───────────────────────────────────────────────────────────

// Derive Roles + ManagedCompanyIds for a UserInfomation row.
// Mirror of migration-user.js — keep these two in lock-step.
function deriveRolesAndCompanies(row) {
  if (row.UserTypeId === 22) {
    return { roles: ['super'], managedCompanyIds: [] };
  }
  const roles = [];
  let managedCompanyIds = [];

  if (row.Production === 1 || row.Production === true) {
    roles.push('manager', 'owner');
    managedCompanyIds = [row.CompanyId];
  }
  if (row.UserTypeId === 24 && !roles.includes('manager')) {
    roles.push('manager');
    managedCompanyIds = [row.CompanyId];
  }
  if (roles.length === 0) {
    roles.push('operator');
  }
  return { roles, managedCompanyIds };
}

// Suburb ("tag1, tag2, …") → ["tag1","tag2",…]. Mirror of migration-controller.js.
function parseTags(suburb) {
  if (!suburb) return [];
  return suburb.split(',').map((t) => t.trim()).filter(Boolean);
}

// AccessLevel → Permission rank. Mirror of migration-assignment.js.
//   0 → 0 (READONLY), 1 → 50 (NORMAL), else → 50.
function mapPermission(accessLevel) {
  if (accessLevel === 0) return 0;
  if (accessLevel === 1) return 50;
  return 50;
}

function toBool(v) {
  return v === 1 || v === true || v === '1' || v === 'true';
}

// ─── UserInfo ─────────────────────────────────────────────────────────────────

async function syncUserInfo(pgClient, mssqlPool) {
  console.log('\n=== Syncing "UserInfo" ===');

  const result = await mssqlPool.request().query(`
    SELECT
      UserId, CompanyId, UserTypeId, Mobile, Email,
      UserName, Name, PasswordHash, Activated, Production,
      LastLoginTime, DateCreated,
      COALESCE(LastUpdateDate, DateCreated) AS LastUpdateDate
    FROM AquariusEmailDB.dbo.UserInfomation
    WHERE Email IS NOT NULL
  `);

  // De-duplicate by lowercased Email (UserInfo has UNIQUE on Email).
  // Priority: Activated=1 > UserTypeId=24 > larger UserId.
  const byEmail = new Map();
  for (const row of result.recordset) {
    const key = (row.Email || '').toLowerCase().trim();
    if (!key) continue;

    const existing = byEmail.get(key);
    if (!existing) { byEmail.set(key, row); continue; }

    const eAct = toBool(existing.Activated);
    const cAct = toBool(row.Activated);
    if (cAct !== eAct) { if (cAct) byEmail.set(key, row); continue; }

    const e24 = existing.UserTypeId === 24;
    const c24 = row.UserTypeId === 24;
    if (c24 !== e24) { if (c24) byEmail.set(key, row); continue; }

    if (row.UserId > existing.UserId) byEmail.set(key, row);
  }
  const rows = [...byEmail.values()];
  console.log(`  Read ${result.recordset.length} source rows → ${rows.length} after email dedup`);

  const validUserIds = new Set();
  let upserted = 0;

  for (const row of rows) {
    const name = row.UserName || row.Name || row.Email;
    const mobile = row.Mobile && String(row.Mobile).trim() !== '' ? row.Mobile : '0000000000';
    const { roles, managedCompanyIds } = deriveRolesAndCompanies(row);

    await pgClient.query(
      `INSERT INTO "UserInfo" (
         "UserId", "CompanyId", "Email", "Mobile", "Name", "PasswordHash",
         "Roles", "ManagedCompanyIds", "Activated",
         "LastLoginTime", "CreatedAt", "UpdatedAt"
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       ON CONFLICT ("UserId") DO UPDATE SET
         "CompanyId"         = EXCLUDED."CompanyId",
         "Email"             = EXCLUDED."Email",
         "Mobile"            = EXCLUDED."Mobile",
         "Name"              = EXCLUDED."Name",
         "PasswordHash"      = EXCLUDED."PasswordHash",
         "Roles"             = EXCLUDED."Roles",
         "ManagedCompanyIds" = EXCLUDED."ManagedCompanyIds",
         "Activated"         = EXCLUDED."Activated",
         "LastLoginTime"     = EXCLUDED."LastLoginTime",
         "UpdatedAt"         = EXCLUDED."UpdatedAt"`,
      [
        row.UserId,
        row.CompanyId,
        row.Email,
        mobile,
        name,
        row.PasswordHash,
        roles,
        managedCompanyIds,
        toBool(row.Activated),
        row.LastLoginTime || null,
        row.DateCreated || new Date(),
        row.LastUpdateDate || new Date(),
      ],
    );
    validUserIds.add(row.UserId);
    upserted++;
  }

  // Delete rows whose UserId no longer appears in the source.
  const keepArr = [...validUserIds];
  const del = await pgClient.query(
    `DELETE FROM "UserInfo"
      WHERE "UserId" <> ALL($1::int[])`,
    [keepArr],
  );
  console.log(`  Upserted ${upserted}, deleted ${del.rowCount} stale row(s)`);
  return validUserIds;
}

// ─── ControllerInfo ───────────────────────────────────────────────────────────

async function syncControllerInfo(pgClient, mssqlPool, validUserIds) {
  console.log('\n=== Syncing "ControllerInfo" ===');

  // Precompute which CompanyIds actually exist in PG (FK target).
  const companyRes = await pgClient.query(`SELECT "CompanyId" FROM "CompanyInfo"`);
  const validCompanyIds = new Set(companyRes.rows.map((r) => r.CompanyId));

  const result = await mssqlPool.request().query(`
    SELECT
      c.UnitId,
      c.SerialNo,
      c.SIMCardNo,
      c.AccManagerId,
      u.CompanyId,
      c.SystemID,
      c.FirmwareVersion,
      c.ControllerModel,
      c.LinuxTimeZoneId,
      c.ProductionStatus,
      c.Activated,
      c.DateCreated,
      c.DateLastUpdate,
      c.SiteLocation,
      c.Suburb,
      u.Email AS BillingEmail,
      c.Barcode,
      c.CustPO,
      c.Serials,
      c.Notes
    FROM AquariusEmailDB.dbo.ControllerInfo     c
    LEFT JOIN AquariusEmailDB.dbo.UserInfomation u
           ON u.UserId = c.AccManagerId
  `);

  const rows = result.recordset;
  console.log(`  Read ${rows.length} source rows`);

  const validUnitIds = new Set();
  let upserted = 0;

  for (const row of rows) {
    const ownerId   = row.AccManagerId && validUserIds.has(row.AccManagerId) ? row.AccManagerId : null;
    const companyId = row.CompanyId    && validCompanyIds.has(row.CompanyId) ? row.CompanyId    : null;

    await pgClient.query(
      `INSERT INTO "ControllerInfo" (
         "UnitId", "SerialNo", "SimCardNo", "OwnerId", "CompanyId",
         "SystemId", "FirmwareVersion", "ModelType", "TimezoneId", "Status",
         "Activated", "DateCreated", "DateLastUpdated",
         "Creator", "Tags", "BillingEmail",
         "BarCode", "PoNumber", "SpareParts", "Notes"
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
       ON CONFLICT ("UnitId") DO UPDATE SET
         "SerialNo"        = EXCLUDED."SerialNo",
         "SimCardNo"       = EXCLUDED."SimCardNo",
         "OwnerId"         = EXCLUDED."OwnerId",
         "CompanyId"       = EXCLUDED."CompanyId",
         "SystemId"        = EXCLUDED."SystemId",
         "FirmwareVersion" = EXCLUDED."FirmwareVersion",
         "ModelType"       = EXCLUDED."ModelType",
         "TimezoneId"      = EXCLUDED."TimezoneId",
         "Status"          = EXCLUDED."Status",
         "Activated"       = EXCLUDED."Activated",
         "DateLastUpdated" = EXCLUDED."DateLastUpdated",
         "Creator"         = EXCLUDED."Creator",
         "Tags"            = EXCLUDED."Tags",
         "BillingEmail"    = EXCLUDED."BillingEmail",
         "BarCode"         = EXCLUDED."BarCode",
         "PoNumber"        = EXCLUDED."PoNumber",
         "SpareParts"      = EXCLUDED."SpareParts",
         "Notes"           = EXCLUDED."Notes"`,
      [
        row.UnitId,
        row.SerialNo || null,
        row.SIMCardNo,
        ownerId,
        companyId,
        row.SystemID,
        row.FirmwareVersion || null,
        row.ControllerModel || null,
        row.LinuxTimeZoneId || null,
        row.ProductionStatus ?? null,
        toBool(row.Activated),
        row.DateCreated    || new Date(),
        row.DateLastUpdate || new Date(),
        row.SiteLocation   || null,
        parseTags(row.Suburb),
        row.BillingEmail   || null,
        row.Barcode        || null,
        row.CustPO         || null,
        row.Serials        || null,
        row.Notes          || null,
      ],
    );
    validUnitIds.add(row.UnitId);
    upserted++;
  }

  const keepArr = [...validUnitIds];
  const del = await pgClient.query(
    `DELETE FROM "ControllerInfo"
      WHERE "UnitId" <> ALL($1::int[])`,
    [keepArr],
  );
  console.log(`  Upserted ${upserted}, deleted ${del.rowCount} stale row(s)`);
  return validUnitIds;
}

// ─── UserControllerAssignment ─────────────────────────────────────────────────

async function syncUserControllerAssignment(pgClient, mssqlPool, validUserIds, validUnitIds) {
  console.log('\n=== Syncing "UserControllerAssignment" ===');

  const result = await mssqlPool.request().query(`
    SELECT SeqKey, UserId, UnitId, AccessLevel, SendSMSAlarm, SendEmailAlarm
    FROM AquariusEmailDB.dbo.UserControllerAssignment
  `);
  const sourceRows = result.recordset;

  // Drop rows whose UserId/UnitId no longer exist — FKs (CASCADE) would fail.
  // Also deduplicate (UserId, UnitId) inside the source: keep the largest SeqKey,
  // matching the behaviour of the UNIQUE (UserId, UnitId) constraint.
  const byPair = new Map();
  let skippedFk = 0;
  for (const row of sourceRows) {
    if (!validUserIds.has(row.UserId) || !validUnitIds.has(row.UnitId)) { skippedFk++; continue; }
    const key = `${row.UserId}|${row.UnitId}`;
    const existing = byPair.get(key);
    if (!existing || row.SeqKey > existing.SeqKey) byPair.set(key, row);
  }
  const rows = [...byPair.values()];
  console.log(
    `  Read ${sourceRows.length} source rows ` +
    `(skipped ${skippedFk} FK-invalid, ` +
    `${sourceRows.length - skippedFk - rows.length} duplicate (UserId,UnitId))`,
  );

  const validIds = new Set();
  let upserted = 0;

  for (const row of rows) {
    await pgClient.query(
      `INSERT INTO "UserControllerAssignment" (
         "Id", "UserId", "UnitId", "Permission", "SendSmsAlarm", "SendEmailAlarm"
       )
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT ("Id") DO UPDATE SET
         "UserId"         = EXCLUDED."UserId",
         "UnitId"         = EXCLUDED."UnitId",
         "Permission"     = EXCLUDED."Permission",
         "SendSmsAlarm"   = EXCLUDED."SendSmsAlarm",
         "SendEmailAlarm" = EXCLUDED."SendEmailAlarm"`,
      [
        row.SeqKey,
        row.UserId,
        row.UnitId,
        mapPermission(row.AccessLevel),
        toBool(row.SendSMSAlarm),
        toBool(row.SendEmailAlarm),
      ],
    );
    validIds.add(row.SeqKey);
    upserted++;
  }

  const keepArr = [...validIds];
  const del = await pgClient.query(
    `DELETE FROM "UserControllerAssignment"
      WHERE "Id" <> ALL($1::int[])`,
    [keepArr],
  );
  console.log(`  Upserted ${upserted}, deleted ${del.rowCount} stale row(s)`);

  // Push the SERIAL sequence past any explicitly-inserted Id values, so
  // app-side inserts (without "Id" in the column list) won't collide.
  await pgClient.query(`
    SELECT setval(
      pg_get_serial_sequence('"UserControllerAssignment"', 'Id'),
      COALESCE((SELECT MAX("Id") FROM "UserControllerAssignment"), 0) + 1,
      false
    );
  `);
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

/**
 * Sync entry point used by index.js.
 *
 * @param {boolean} closePool  — close the PG pool after syncing (use when
 *                               running this script standalone, not on the
 *                               long-lived scheduled-sync path).
 * The other two positional arguments exist for backward compatibility with
 * older callers and are ignored:
 *   - isFullSync   (sync is always a full upsert + orphan cleanup)
 *   - rebuildSchema (schema rebuild lives in src/initDatabase.js)
 */
async function syncAllTables(closePool = false, _isFullSync = false, _rebuildSchema = false) {
  const pgClient = await pgPool.connect();
  let mssqlPool;

  try {
    console.log('='.repeat(60));
    console.log('Sync run starting…');
    console.log('='.repeat(60));

    mssqlPool = await sql.connect(sqlConfig);
    console.log(`✓ Connected to SQL Server: ${sqlConfig.server}/${sqlConfig.database}`);

    await pgClient.query(`SELECT 1`);
    console.log('✓ Connected to PostgreSQL');

    await pgClient.query('BEGIN');

    // Order matters for FK visibility within the transaction:
    //   UserInfo → ControllerInfo → UserControllerAssignment
    const validUserIds = await syncUserInfo(pgClient, mssqlPool);
    const validUnitIds = await syncControllerInfo(pgClient, mssqlPool, validUserIds);
    await syncUserControllerAssignment(pgClient, mssqlPool, validUserIds, validUnitIds);

    await pgClient.query('COMMIT');

    console.log('\n' + '='.repeat(60));
    console.log('✓ Sync run completed');
    console.log('='.repeat(60));
  } catch (err) {
    await pgClient.query('ROLLBACK').catch(() => {});
    console.error('\n✗ Sync run failed — rolled back:', err.message);
    console.error(err.stack);
    throw err;
  } finally {
    if (mssqlPool) await mssqlPool.close().catch(() => {});
    pgClient.release();
    if (closePool) await pgPool.end().catch(() => {});
  }
}

export { syncAllTables };

// ─── Standalone entry ─────────────────────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const isMainModule = process.argv[1] && resolve(process.argv[1]) === resolve(__filename);

if (isMainModule) {
  syncAllTables(true).catch(() => process.exit(1));
}
