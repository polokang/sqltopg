/**
 * migration-controller.js
 *
 * Migrate ControllerInfo: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : AquariusEmailDB.dbo.ControllerInfo  (LEFT JOIN UserInfomation for CompanyId & BillingEmail)
 * Target : PostgreSQL public."ControllerInfo"
 *
 * Column mapping:
 *   c.UnitId            → "UnitId"
 *   c.SerialNo          → "SerialNo"
 *   c.SIMCardNo         → "SimCardNo"
 *   c.AccManagerId      → "OwnerId"
 *   u.CompanyId         → "CompanyId"   (via UserInfomation WHERE UserId = AccManagerId)
 *   c.SystemID          → "SystemId"
 *   c.FirmwareVersion   → "FirmwareVersion"
 *   c.ControllerModel   → "ModelType"
 *   c.LinuxTimeZoneId   → "TimezoneId"
 *   c.ProductionStatus  → "Status"
 *   c.Activated         → "Activated"
 *   c.DateCreated       → "DateCreated"
 *   c.DateLastUpdate    → "DateLastUpdated"
 *   c.SiteLocation      → "Creator"
 *   c.Suburb            → "Tags"        (split by "," if not NULL)
 *   u.Email             → "BillingEmail" (via UserInfomation WHERE UserId = AccManagerId)
 *   c.Barcode           → "BarCode"
 *   c.CustPO            → "PoNumber"
 *   c.Serials           → "SpareParts"
 *   c.Notes             → "Notes"
 *
 * Run:
 *   node src/migration-controller.js
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
    if (k === 'database')                    cfg.database = v;
    else if (k === 'user')                   cfg.user = v;
    else if (k === 'password')               cfg.password = v;
    else if (k === 'trustservercertificate') cfg.options.trustServerCertificate = v === 'true';
  }

  return cfg;
}

const sqlConfig = parseSqlServerUrl(process.env.SQL_SERVER_URL);

// ─── Helpers ──────────────────────────────────────────────────────────────────

// Parse Suburb → Tags array (split by comma, trim each element, drop empty strings)
function parseTags(suburb) {
  if (!suburb) return [];
  return suburb
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean);
}

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateControllerInfo(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  // LEFT JOIN UserInfomation to resolve CompanyId and BillingEmail from AccManagerId
  const result = await sqlPool.request().query(`
    SELECT
      c.[UnitId],
      c.[SerialNo],
      c.[SIMCardNo],
      c.[AccManagerId],
      u.[CompanyId],
      c.[SystemID],
      c.[FirmwareVersion],
      c.[ControllerModel],
      c.[LinuxTimeZoneId],
      c.[ProductionStatus],
      c.[Activated],
      c.[DateCreated],
      c.[DateLastUpdate],
      c.[SiteLocation],
      c.[Suburb],
      u.[Email]     AS BillingEmail,
      c.[Barcode],
      c.[CustPO],
      c.[Serials],
      c.[Notes]
    FROM  AquariusEmailDB.dbo.ControllerInfo     c
    LEFT JOIN AquariusEmailDB.dbo.UserInfomation u
           ON u.[UserId] = c.[AccManagerId]
  `);

  const rows = result.recordset;
  console.log(`✓ Read ${rows.length} rows from ControllerInfo`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  // Temporarily drop FK constraints — referenced rows in UserInfo / CompanyInfo
  // may not yet cover every value present in ControllerInfo.
  // Both constraints are restored at the end of this function.
  await pgClient.query(`
    ALTER TABLE "ControllerInfo"
      DROP CONSTRAINT IF EXISTS "ControllerInfo_OwnerId_fkey",
      DROP CONSTRAINT IF EXISTS "ControllerInfo_CompanyId_fkey";
  `);
  console.log('✓ Temporarily dropped ControllerInfo FK constraints');

  let inserted = 0;
  const progress = startProgress(rows.length, 'ControllerInfo');

  for (const row of rows) {
    await pgClient.query(
      `INSERT INTO "ControllerInfo" (
         "UnitId",
         "SerialNo",
         "SimCardNo",
         "OwnerId",
         "CompanyId",
         "SystemId",
         "FirmwareVersion",
         "ModelType",
         "TimezoneId",
         "Status",
         "Activated",
         "DateCreated",
         "DateLastUpdated",
         "Creator",
         "Tags",
         "BillingEmail",
         "BarCode",
         "PoNumber",
         "SpareParts",
         "Notes"
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
      [
        row.UnitId,
        row.SerialNo    || null,
        row.SIMCardNo,
        row.AccManagerId || null,                                  // OwnerId
        row.CompanyId    || null,                                  // from UserInfomation JOIN
        row.SystemID,
        row.FirmwareVersion || null,
        row.ControllerModel || null,                               // ModelType
        row.LinuxTimeZoneId || null,                               // TimezoneId
        row.ProductionStatus ?? null,                              // Status
        row.Activated === 1 || row.Activated === true,
        row.DateCreated      || null,
        row.DateLastUpdate   || null,                              // DateLastUpdated
        row.SiteLocation     || null,                              // Creator
        parseTags(row.Suburb),                                     // Tags
        row.BillingEmail     || null,
        row.Barcode          || null,                              // BarCode
        row.CustPO           || null,                             // PoNumber
        row.Serials          || null,                              // SpareParts
        row.Notes            || null,
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);

  // ── Diagnose orphaned references before restoring FK constraints ─────────────
  const orphanCompany = await pgClient.query(`
    SELECT c."UnitId", c."CompanyId"
    FROM   "ControllerInfo" c
    WHERE  c."CompanyId" IS NOT NULL
      AND  NOT EXISTS (
        SELECT 1 FROM "CompanyInfo" p WHERE p."CompanyId" = c."CompanyId"
      )
    ORDER BY c."CompanyId";
  `);
  if (orphanCompany.rows.length > 0) {
    console.warn(`\n⚠ ${orphanCompany.rows.length} row(s) in "ControllerInfo" reference a "CompanyId" not found in "CompanyInfo":`);
    console.warn('  UnitId\t\tCompanyId');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanCompany.rows) {
      console.warn(`  ${r.UnitId}\t\t${r.CompanyId}`);
    }
  }

  const orphanOwner = await pgClient.query(`
    SELECT c."UnitId", c."OwnerId"
    FROM   "ControllerInfo" c
    WHERE  c."OwnerId" IS NOT NULL
      AND  NOT EXISTS (
        SELECT 1 FROM "UserInfo" u WHERE u."UserId" = c."OwnerId"
      )
    ORDER BY c."OwnerId";
  `);
  if (orphanOwner.rows.length > 0) {
    console.warn(`\n⚠ ${orphanOwner.rows.length} row(s) in "ControllerInfo" reference an "OwnerId" not found in "UserInfo":`);
    console.warn('  UnitId\t\tOwnerId');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanOwner.rows) {
      console.warn(`  ${r.UnitId}\t\t${r.OwnerId}`);
    }
  }

  if (orphanCompany.rows.length > 0 || orphanOwner.rows.length > 0) {
    throw new Error(
      `Cannot restore FK constraints: found ${orphanCompany.rows.length} orphaned CompanyId(s) ` +
      `and ${orphanOwner.rows.length} orphaned OwnerId(s). See details above.`
    );
  }

  // Restore FK constraints
  await pgClient.query(`
    ALTER TABLE "ControllerInfo"
      ADD CONSTRAINT "ControllerInfo_OwnerId_fkey"
        FOREIGN KEY ("OwnerId")   REFERENCES "UserInfo"    ("UserId")    ON DELETE SET NULL,
      ADD CONSTRAINT "ControllerInfo_CompanyId_fkey"
        FOREIGN KEY ("CompanyId") REFERENCES "CompanyInfo" ("CompanyId") ON DELETE SET NULL;
  `);
  console.log('✓ Restored ControllerInfo FK constraints');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateControllerInfo(pgClient);
    await pgClient.query('COMMIT');

    console.log('\n✓ migration-controller completed.');
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
