/**
 * migration-company.js
 *
 * Migrate CompanyInfo: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source  : AquariusEmailDB.dbo.UserInfomation  WHERE Production=1 (all Activated)
 * Target  : PostgreSQL "CompanyInfo"
 *
 * Column mapping:
 *   CompanyId      → "CompanyId"    (preserved as-is)
 *   UserName       → "CompanyName"  AND "Desc"
 *   Logo           → "CompanyLogo"
 *   UserId         → "ManagerId"    (FK constraint temporarily dropped; restored after UserInfo migration)
 *   Mobile         → "Phone"
 *   Email          → "Email"
 *   DateCreated    → "CreatedAt"
 *   LastUpdateDate → "UpdatedAt"
 *   JWCode         → "JwCode"
 *
 * Run:
 *   node src/migration-company.js
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
    if (k === 'database')               cfg.database = v;
    else if (k === 'user')              cfg.user = v;
    else if (k === 'password')          cfg.password = v;
    else if (k === 'trustservercertificate')
      cfg.options.trustServerCertificate = v === 'true';
  }

  return cfg;
}

const sqlConfig = parseSqlServerUrl(process.env.SQL_SERVER_URL);

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateCompanyInfo(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected to SQL Server: ${sqlConfig.server}/${sqlConfig.database}`);

  const result = await sqlPool.request().query(`
    SELECT
      CompanyId,
      UserName,
      Logo,
      UserId,
      Mobile,
      Email,
      DateCreated,
      LastUpdateDate,
      JWCode
    FROM AquariusEmailDB.dbo.UserInfomation
    WHERE Production = 1
  `);

  const rows = result.recordset;
  console.log(`✓ Read ${rows.length} rows from UserInfomation (Production=1, all Activated)`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  // Drop FK temporarily — UserInfo is empty at this stage so ManagerId values
  // would violate the constraint. Restore the FK after UserInfo is migrated.
  await pgClient.query(`
    ALTER TABLE "CompanyInfo"
      DROP CONSTRAINT IF EXISTS "CompanyInfo_ManagerId_fkey"
  `);
  console.log('✓ Temporarily dropped CompanyInfo_ManagerId_fkey');

  let inserted = 0;
  const progress = startProgress(rows.length, 'CompanyInfo  ');

  for (const row of rows) {
    await pgClient.query(
        `INSERT INTO "CompanyInfo"
           ("CompanyId", "CompanyName", "Desc", "CompanyLogo",
            "ManagerId", "Phone", "Email", "CreatedAt", "UpdatedAt", "JwCode")
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT ("CompanyId") DO NOTHING`,
        [
          row.CompanyId,
          row.UserName,                              // CompanyName
          row.UserName,                              // Desc — same as CompanyName per spec
          row.Logo || 'YourLogoGoesToHere.JPG',      // CompanyLogo
          row.UserId,                                // ManagerId — raw UserId; FK re-added after UserInfo migration
          row.Mobile || null,                        // Phone
          row.Email || null,                         // Email
          row.DateCreated,
          row.LastUpdateDate,
          row.JWCode || null,
        ],
      );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);
}

// ─── Manual inserts ───────────────────────────────────────────────────────────

async function insertManualRows(pgClient) {
  const rows = [
    // CompanyId, CompanyName, ManagerId, JwCode, Desc, Phone, Email, CompanyLogo, Activated, CreatedAt, UpdatedAt
    [3066, 'Rare-Enviro', 9092, 'RAR001', 'Rare-Enviro', '0428220648', 'Richard@rare-enviro.com.au', 'YourLogoGoesToHere.JPG', true, '2023-11-13 15:51:48.500', '2023-11-13 15:51:48.500'],
  ];

  for (const r of rows) {
    await pgClient.query(
      `INSERT INTO "CompanyInfo"
         ("CompanyId", "CompanyName", "ManagerId", "JwCode", "Desc",
          "Phone", "Email", "CompanyLogo", "Activated", "CreatedAt", "UpdatedAt")
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT ("CompanyId") DO NOTHING`,
      r,
    );
    console.log(`  ✓ Manual insert: CompanyId=${r[0]} "${r[1]}"`);
  }
}

// ─── Reset sequence so next SERIAL value starts above the migrated max ────────

async function resetCompanyIdSeq(pgClient) {
  await pgClient.query(`
    SELECT setval(
      pg_get_serial_sequence('"CompanyInfo"', 'CompanyId'),
      COALESCE((SELECT MAX("CompanyId") FROM "CompanyInfo"), 0) + 1,
      false
    );
  `);
  console.log('✓ CompanyId sequence reset');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateCompanyInfo(pgClient);
    await insertManualRows(pgClient);
    await pgClient.query('COMMIT');

    await resetCompanyIdSeq(pgClient);

    console.log('\n✓ migration-company completed.');
    console.log('Next: migrate UserInfo, then restore the FK constraint:');
    console.log('  ALTER TABLE "CompanyInfo" ADD CONSTRAINT "CompanyInfo_ManagerId_fkey"');
    console.log('    FOREIGN KEY ("ManagerId") REFERENCES "UserInfo" ("UserId") ON DELETE CASCADE;');
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
