/**
 * migration-company.js
 *
 * Migrate companies: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source  : AquariusEmailDB.dbo.CompanyInfomation   （185 家，全量）
 * Target  : PostgreSQL public.companies
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md
 * 目标 schema：src/initDatabase.js　派生规则：src/derive.js
 *
 * Column mapping:
 *   CompanyId       → company_id        (原值保留，序列最后 setval 到 MAX+1)
 *   CompanyName     → name              (真名)
 *   Phone           → phone
 *   Email           → email
 *   CompanyLogoName → logo_url
 *   Activated       → activated
 *   DateCreated     → created_at
 *   LastUpdateDate  → updated_at        (源列可空，空则回退 DateCreated)
 *   —               → manager_user_id   该公司的 UserTypeId=24 用户，见下
 *   —               → description       不再填（旧版填的是公司名，无信息量）
 *
 * ⚠️ 2026-09-09 重写：数据源从 `UserInfomation WHERE Production=1` 换成
 *    `dbo.CompanyInfomation`。旧做法的三个问题：
 *      ① 只得到 146 家（185 - 39），而那 39 家被 181 个用户和 52 台设备引用着；
 *      ② 公司名取的是**用户的 UserName**，同一 CompanyId 有多个 Production 用户时
 *         名字取决于谁先插入 —— CompanyId 2006 实为 Integra，却有一半概率被命名成
 *         "TerraCom Resources"（完全另一家公司）；
 *      ③ 需要 jw_code 列，而邮件明确 "Companies don't need a jiwa code"。
 *    换源之后，controllers/users 对 companies 的引用零悬空（已核实）。
 *
 * manager_user_id 取该公司的 `UserTypeId = 24`（CorporateManager）用户：
 *   生产数据上 171 个 CorporateManager 恰好分布在 171 家不同公司（一家一个），
 *   所以这个取值无歧义。剩下 14 家没有 CorporateManager 的填 NULL。
 *   外键先 drop、插完诊断孤儿再 restore（users 里可能缺行 —— Email 为空的会被
 *   migration-user.js 丢弃）。
 *
 * Run:
 *   node src/migration-company.js      ← 必须在 migration-user.js 之后
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { startProgress } from './progress.js';
import { USER_TYPE, toBool, nullIfBlank } from './derive.js';

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
const SRC_DB = sqlConfig.database || 'AquariusEmailDB';

// ─── Pre-flight ───────────────────────────────────────────────────────────────

/**
 * companies.name 是 NOT NULL（**没有 UNIQUE**，见 initDatabase.js 的说明）。
 *
 *   空名  → 致命。NOT NULL 会直接拒绝，且没有合理的兜底值。
 *   重名  → 只告警。源库确实有重名（'Rockhampton Regional Council' 的 2036/2092），
 *           公司身份是 company_id，代码从不按 name 查询。同步服务不能因为源库的
 *           一个合法状态就停摆。
 */
function checkNames(rows) {
  const blank = rows.filter((r) => (r.CompanyName ?? '').trim() === '');
  if (blank.length > 0) {
    console.error(`\n✗ ${blank.length} 家公司的 CompanyName 为空，而目标列是 NOT NULL：`);
    for (const r of blank) console.error(`  CompanyId=${r.CompanyId}`);
    console.error('\n请先在 SQL Server 上补齐名称，再重跑本脚本。');
    throw new Error(`${blank.length} company name(s) are blank — aborted before writing anything.`);
  }

  const seen = new Map();
  for (const row of rows) {
    const key = (row.CompanyName ?? '').trim();
    if (!seen.has(key)) seen.set(key, []);
    seen.get(key).push(row.CompanyId);
  }
  const dups = [...seen.entries()].filter(([, ids]) => ids.length > 1);
  if (dups.length > 0) {
    console.warn(`\n⚠ ${dups.length} 组公司重名（不影响迁移，company_id 才是身份）：`);
    for (const [name, ids] of dups) {
      console.warn(`  ${name} → CompanyId ${ids.join(', ')}`);
    }
    console.warn('  建议在 SQL Server 上确认哪一条有效、把另一条停用。\n');
  }
}

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateCompanies(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected to SQL Server: ${sqlConfig.server}/${sqlConfig.database}`);

  // 每家公司一行，附带该公司的 CorporateManager（UserTypeId=24）。
  // 用 OUTER APPLY + TOP 1 是为了万一某家有多个 24（生产数据上没有），取 UserId 最小的，
  // 结果确定、可重复 —— 与 sync-simple.js 的同一段查询保持一致。
  const result = await sqlPool.request().query(`
    SELECT
      c.CompanyId,
      c.CompanyName,
      c.Phone,
      c.Email,
      c.CompanyLogoName,
      c.Activated,
      c.DateCreated,
      c.LastUpdateDate,
      m.UserId AS ManagerUserId
    FROM ${SRC_DB}.dbo.CompanyInfomation c
    OUTER APPLY (
      SELECT TOP 1 u.UserId
        FROM ${SRC_DB}.dbo.UserInfomation u
       WHERE u.CompanyId = c.CompanyId
         AND u.UserTypeId = ${USER_TYPE.CORPORATE_MANAGER}
         AND u.Email IS NOT NULL
       ORDER BY u.UserId
    ) m
    ORDER BY c.CompanyId
  `);

  const rows = result.recordset;
  const withManager = rows.filter((r) => r.ManagerUserId != null).length;
  console.log(`✓ Read ${rows.length} rows from CompanyInfomation（${withManager} 家有 CorporateManager）`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  checkNames(rows);

  // manager_user_id 指向 users，而 users 里可能缺行（Email 为空的被丢弃了）。
  // 先 drop 外键，插完诊断孤儿再 restore。
  await pgClient.query(`
    ALTER TABLE companies
      DROP CONSTRAINT IF EXISTS companies_manager_user_id_fkey
  `);
  console.log('✓ Temporarily dropped companies_manager_user_id_fkey');

  let inserted = 0;
  const progress = startProgress(rows.length, 'companies    ');

  for (const row of rows) {
    const res = await pgClient.query(
      `INSERT INTO companies
         (company_id, name, manager_user_id, phone, email,
          logo_url, activated, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (company_id) DO NOTHING`,
      [
        row.CompanyId,
        String(row.CompanyName).trim(),                       // name（真名）
        row.ManagerUserId ?? null,                            // manager_user_id
        nullIfBlank(row.Phone),                               // phone
        nullIfBlank(row.Email),                               // email
        nullIfBlank(row.CompanyLogoName) ?? 'YourLogoGoesToHere.JPG',
        toBool(row.Activated),                                // activated
        row.DateCreated,                                      // created_at
        row.LastUpdateDate || row.DateCreated,                // updated_at（源列可空）
      ],
    );
    inserted += res.rowCount;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted ${inserted} / skipped ${rows.length - inserted}（已存在的行）`);
}

// ─── Restore the FK dropped above ─────────────────────────────────────────────

async function restoreManagerFk(pgClient) {
  const orphan = await pgClient.query(`
    SELECT c.company_id, c.manager_user_id
      FROM companies c
     WHERE c.manager_user_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = c.manager_user_id)
     ORDER BY c.company_id;
  `);

  if (orphan.rows.length > 0) {
    console.warn(`\n⚠ ${orphan.rows.length} 家公司的 manager_user_id 在 users 里找不到`);
    console.warn('  （通常是该 CorporateManager 的 Email 为空、被 migration-user.js 丢弃了）');
    console.warn('  company_id\tmanager_user_id');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphan.rows) {
      console.warn(`  ${r.company_id}\t\t${r.manager_user_id}`);
    }
    throw new Error(
      `Cannot restore companies_manager_user_id_fkey: ${orphan.rows.length} orphaned manager_user_id(s). See above.`,
    );
  }

  // ON DELETE SET NULL —— 与 initDatabase.js 一致。
  // （旧脚本恢复成 CASCADE 是笔误：删一个用户会连带删掉他管理的整家公司，
  //   再经 controllers.company_id 的 SET NULL 让该公司所有设备失去归属。）
  await pgClient.query(`
    ALTER TABLE companies
      ADD CONSTRAINT companies_manager_user_id_fkey
        FOREIGN KEY (manager_user_id) REFERENCES users(user_id) ON DELETE SET NULL;
  `);
  console.log('✓ Restored companies_manager_user_id_fkey → users(user_id) ON DELETE SET NULL');
}

// ─── Reset sequence so next SERIAL value starts above the migrated max ────────

async function resetCompanyIdSeq(pgClient) {
  await pgClient.query(`
    SELECT setval(
      pg_get_serial_sequence('companies', 'company_id'),
      COALESCE((SELECT MAX(company_id) FROM companies), 0) + 1,
      false
    );
  `);
  console.log('✓ companies.company_id sequence reset');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateCompanies(pgClient);
    await restoreManagerFk(pgClient);
    await pgClient.query('COMMIT');

    await resetCompanyIdSeq(pgClient);

    console.log('\n✓ migration-company completed.');
    console.log('Next: node src/migration-controller.js');
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
