/**
 * migration-assignment.js
 *
 * Migrate UserControllerAssignment: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : AquariusEmailDB.dbo.UserControllerAssignment
 * Target : PostgreSQL public."UserControllerAssignment"
 *
 * Column mapping:
 *   SeqKey           → "Id"
 *   UserId           → "UserId"
 *   UnitId           → "UnitId"
 *   AccessLevel      → "Permission"     (0 → 0 READONLY, 1 → 50 NORMAL, else → 50)
 *   SendSMSAlarm     → "SendSmsAlarm"
 *   SendEmailAlarm   → "SendEmailAlarm"
 *
 * Permission values (see initDatabase.js):
 *     0   READONLY
 *    50   NORMAL   (default)
 *   100   OWNER
 *
 * Run:
 *   node src/migration-assignment.js
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

// Map AccessLevel → Permission rank.
//   0 → 0   (READONLY)
//   1 → 50  (NORMAL)
//   anything else → 50 (NORMAL default)
function mapPermission(accessLevel) {
  if (accessLevel === 0) return 0;
  if (accessLevel === 1) return 50;
  return 50;
}

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateAssignment(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  const result = await sqlPool.request().query(`
    SELECT
      [SeqKey],
      [UserId],
      [UnitId],
      [AccessLevel],
      [SendSMSAlarm],
      [SendEmailAlarm]
    FROM AquariusEmailDB.dbo.UserControllerAssignment
  `);

  const rows = result.recordset;
  console.log(`✓ Read ${rows.length} rows from UserControllerAssignment`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  // Temporarily drop FK + unique constraints — the source may contain
  // (UserId, UnitId) pairs that don't exist in the migrated UserInfo /
  // ControllerInfo tables, and it may also contain duplicates.
  await pgClient.query(`
    ALTER TABLE "UserControllerAssignment"
      DROP CONSTRAINT IF EXISTS "UserControllerAssignment_UserId_fkey",
      DROP CONSTRAINT IF EXISTS "UserControllerAssignment_UnitId_fkey";
  `);
  console.log('✓ Temporarily dropped UserControllerAssignment FK constraints');

  let inserted = 0;
  const progress = startProgress(rows.length, 'UserControllerAssignment');

  for (const row of rows) {
    await pgClient.query(
      `INSERT INTO "UserControllerAssignment" (
         "Id",
         "UserId",
         "UnitId",
         "Permission",
         "SendSmsAlarm",
         "SendEmailAlarm"
       )
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        row.SeqKey,
        row.UserId,
        row.UnitId,
        mapPermission(row.AccessLevel),
        row.SendSMSAlarm === 1 || row.SendSMSAlarm === true,
        row.SendEmailAlarm === 1 || row.SendEmailAlarm === true,
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);

  // ── Diagnose orphaned references before restoring FK constraints ─────────
  const orphanUser = await pgClient.query(`
    SELECT a."Id", a."UserId"
    FROM   "UserControllerAssignment" a
    WHERE  NOT EXISTS (SELECT 1 FROM "UserInfo" u WHERE u."UserId" = a."UserId")
    ORDER BY a."UserId";
  `);
  if (orphanUser.rows.length > 0) {
    console.warn(`\n⚠ ${orphanUser.rows.length} row(s) reference a "UserId" not found in "UserInfo":`);
    console.warn('  Id\t\tUserId');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanUser.rows) {
      console.warn(`  ${r.Id}\t\t${r.UserId}`);
    }
  }

  const orphanUnit = await pgClient.query(`
    SELECT a."Id", a."UnitId"
    FROM   "UserControllerAssignment" a
    WHERE  NOT EXISTS (SELECT 1 FROM "ControllerInfo" c WHERE c."UnitId" = a."UnitId")
    ORDER BY a."UnitId";
  `);
  if (orphanUnit.rows.length > 0) {
    console.warn(`\n⚠ ${orphanUnit.rows.length} row(s) reference a "UnitId" not found in "ControllerInfo":`);
    console.warn('  Id\t\tUnitId');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanUnit.rows) {
      console.warn(`  ${r.Id}\t\t${r.UnitId}`);
    }
  }

  if (orphanUser.rows.length > 0 || orphanUnit.rows.length > 0) {
    throw new Error(
      `Cannot restore FK constraints: found ${orphanUser.rows.length} orphaned UserId(s) ` +
      `and ${orphanUnit.rows.length} orphaned UnitId(s). See details above.`
    );
  }

  // Restore FK constraints
  await pgClient.query(`
    ALTER TABLE "UserControllerAssignment"
      ADD CONSTRAINT "UserControllerAssignment_UserId_fkey"
        FOREIGN KEY ("UserId") REFERENCES "UserInfo"       ("UserId") ON DELETE CASCADE,
      ADD CONSTRAINT "UserControllerAssignment_UnitId_fkey"
        FOREIGN KEY ("UnitId") REFERENCES "ControllerInfo" ("UnitId") ON DELETE CASCADE;
  `);
  console.log('✓ Restored UserControllerAssignment FK constraints');
}

// ─── Reset SERIAL sequence so the next auto-generated Id > max(Id) ────────────

async function resetIdSeq(pgClient) {
  await pgClient.query(`
    SELECT setval(
      pg_get_serial_sequence('"UserControllerAssignment"', 'Id'),
      COALESCE((SELECT MAX("Id") FROM "UserControllerAssignment"), 0) + 1,
      false
    );
  `);
  console.log('✓ UserControllerAssignment.Id sequence reset');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateAssignment(pgClient);
    await pgClient.query('COMMIT');

    await resetIdSeq(pgClient);

    console.log('\n✓ migration-assignment completed.');
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
