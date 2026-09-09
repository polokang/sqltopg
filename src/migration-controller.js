/**
 * migration-controller.js
 *
 * Migrate controllers: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : AquariusEmailDB.dbo.ControllerInfo  (LEFT JOIN UserInfomation → 公司与账单邮箱)
 * Target : PostgreSQL public.controllers
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md（snake_case）
 * 目标 schema：src/initDatabase.js
 *
 * Column mapping:
 *   c.UnitId            → unit_id
 *   c.SerialNo          → serial_no
 *   c.SIMCardNo         → sim_card_no        (NOT NULL)
 *   c.AccManagerId      → owner_user_id
 *   c.CompanyId         → company_id         设备自己的公司（"由哪家公司管理"）
 *   c.SystemID          → system_id          (NOT NULL)
 *   c.FirmwareVersion   → firmware_version
 *   c.ControllerModel   → model_type
 *   c.LinuxTimeZoneId   → timezone_id
 *   c.ProductionStatus  → production_status  (旧列名叫 "Status"，语义已正名)
 *   c.Activated         → activated
 *   c.DateCreated       → created_at         (NOT NULL)
 *   c.DateLastUpdate    → updated_at         (NOT NULL；源列可空，空则回退到 DateCreated)
 *   c.SiteLocation      → site_location      (旧列名叫 "Creator"，语义已正名)
 *   c.Suburb            → tags               (按 "," 拆成数组)
 *   u.Email             → billing_email      ⚠️ 派生值，源库没有这一列（O3）
 *   c.Barcode           → bar_code
 *   c.CustPO            → po_number
 *   c.Serials           → spare_parts
 *   c.Notes             → notes
 *   —                   → sync_source        (建表 DEFAULT 'mssql'，不显式写)
 *
 * ✅ O2 / 台账 R-003 已修（2026-09-09）：
 *    company_id 改取 `c.CompanyId`（设备自己的公司），不再取 Owner 所属公司。
 *    依据是 David Grevell 的权限模型邮件 —— `ControllerInfo.companyId` 的语义就是
 *    "这台设备由哪家公司管理"，而它正是 Manager 权限的判据
 *    （`managed_company_ids ∋ company_id`）。生产数据里两者有 223 台不同，取错会让
 *    Pro 与老站的 Manager 可见范围不一致。
 *
 *    保留 LEFT JOIN UserInfomation 只为取 billing_email（Owner 的邮箱）。
 *
 * ⚠️ billing_email 仍是**派生值** —— SQL Server 的 dbo.ControllerInfo 没有这一列。
 *    按邮件它应该是 ControllerInfo 上的一个真列（"just an email address, doesn't
 *    require a user to be created"）。加列前 Pro 改了这个字段会被同步覆盖回
 *    Owner 邮箱。见台账 R-004。
 *
 * Run:
 *   node src/migration-controller.js
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { startProgress } from './progress.js';
import { parseTags, toBool } from './derive.js';
import { getSqlServerConfig, getPostgresUrl } from './dbConfig.js';

dotenv.config();

// ─── PostgreSQL connection ────────────────────────────────────────────────────

const pgPool = new Pool({
  connectionString: getPostgresUrl(),
  ssl: { rejectUnauthorized: false },
});

// ─── SQL Server connection ────────────────────────────────────────────────────

const { cfg: sqlConfig, srcDb: SRC_DB } = getSqlServerConfig();

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * sim_card_no / system_id / created_at 在目标表上都是 NOT NULL，而源库对应列可空
 * （DateCreated 按 Prisma schema 是 NOT NULL，这里一并兜住，免得哪天源库放宽了约束
 *  就变成难查的运行时错误）。
 *
 * 有空值就先报清楚再退出，而不是让 pg 抛一句只有列名的 not-null violation。
 *
 * ⚠️ updated_at 不在这里检查 —— 源列 DateLastUpdate 本来就可空，插入时回退到
 *    DateCreated（语义：从未更新过的设备，updated_at = created_at）。
 *    注意不能靠目标列的 DEFAULT NOW() 兜底：**显式传 NULL 不会触发 DEFAULT**，
 *    只有整列省略才会。2026-09-08 的排练就是在这里炸的。
 */
function checkRequiredColumns(rows) {
  const bad = rows.filter(
    (r) => r.SIMCardNo == null || r.SystemID == null || r.DateCreated == null,
  );
  if (bad.length === 0) return;

  console.error(`\n✗ ${bad.length} 台设备的 SIMCardNo / SystemID / DateCreated 为空，而目标列是 NOT NULL：`);
  console.error('  UnitId\t\tSIMCardNo\t\tSystemID\t\tDateCreated');
  console.error('  ' + '-'.repeat(72));
  for (const r of bad.slice(0, 50)) {
    console.error(
      `  ${r.UnitId}\t\t${r.SIMCardNo ?? '<空>'}\t\t${r.SystemID ?? '<空>'}\t\t${r.DateCreated ?? '<空>'}`,
    );
  }
  if (bad.length > 50) console.error(`  …还有 ${bad.length - 50} 行`);
  console.error('\n两种处置：① 在 SQL Server 上补齐这些值；② 确认这些设备是废弃记录，');
  console.error('在本脚本的 SELECT 里过滤掉。不要在这里默默填空串 —— 那会让废数据看起来正常。');
  throw new Error(`${bad.length} row(s) violate NOT NULL — aborted before writing anything.`);
}

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateControllers(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  // LEFT JOIN UserInfomation：从 AccManagerId 解析出公司与账单邮箱
  const result = await sqlPool.request().query(`
    SELECT
      c.[UnitId],
      c.[SerialNo],
      c.[SIMCardNo],
      c.[AccManagerId],
      c.[CompanyId],
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
    FROM  ${SRC_DB}.dbo.ControllerInfo     c
    LEFT JOIN ${SRC_DB}.dbo.UserInfomation u
           ON u.[UserId] = c.[AccManagerId]
  `);

  const rows = result.recordset;
  console.log(`✓ Read ${rows.length} rows from ControllerInfo`);
  await sqlPool.close();

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  checkRequiredColumns(rows);

  // 先 drop 外键：users / companies 里未必覆盖 ControllerInfo 出现的每个值。
  // 插完后诊断孤儿，再 restore（见本函数末尾）。
  await pgClient.query(`
    ALTER TABLE controllers
      DROP CONSTRAINT IF EXISTS controllers_owner_user_id_fkey,
      DROP CONSTRAINT IF EXISTS controllers_company_id_fkey;
  `);
  console.log('✓ Temporarily dropped controllers FK constraints');

  let inserted = 0;
  const progress = startProgress(rows.length, 'controllers  ');

  for (const row of rows) {
    await pgClient.query(
      `INSERT INTO controllers (
         unit_id,
         serial_no,
         sim_card_no,
         owner_user_id,
         company_id,
         system_id,
         firmware_version,
         model_type,
         timezone_id,
         production_status,
         activated,
         created_at,
         updated_at,
         site_location,
         tags,
         billing_email,
         bar_code,
         po_number,
         spare_parts,
         notes
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
      [
        row.UnitId,
        row.SerialNo         || null,
        row.SIMCardNo,
        row.AccManagerId     || null,                   // owner_user_id
        row.CompanyId        || null,                   // 设备自己的公司（R-003 已修）
        row.SystemID,
        row.FirmwareVersion  || null,
        row.ControllerModel  || null,                   // model_type
        row.LinuxTimeZoneId  || null,                   // timezone_id
        row.ProductionStatus ?? null,                   // production_status
        toBool(row.Activated),
        row.DateCreated,                                // created_at（NOT NULL，已在前置检查兜住）
        row.DateLastUpdate || row.DateCreated,          // updated_at（NOT NULL，源列可空则回退）
        row.SiteLocation     || null,                   // site_location
        parseTags(row.Suburb),                          // tags
        row.BillingEmail     || null,                   // billing_email（派生，见 O3）
        row.Barcode          || null,                   // bar_code
        row.CustPO           || null,                   // po_number
        row.Serials          || null,                   // spare_parts
        row.Notes            || null,
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);

  // ── 恢复外键前先诊断孤儿引用 ────────────────────────────────────────────────
  const orphanCompany = await pgClient.query(`
    SELECT c.unit_id, c.company_id
      FROM controllers c
     WHERE c.company_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM companies p WHERE p.company_id = c.company_id)
     ORDER BY c.company_id;
  `);
  if (orphanCompany.rows.length > 0) {
    console.warn(`\n⚠ ${orphanCompany.rows.length} 行 controllers 的 company_id 在 companies 里找不到：`);
    console.warn('  unit_id\t\tcompany_id');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanCompany.rows) {
      console.warn(`  ${r.unit_id}\t\t${r.company_id}`);
    }
  }

  const orphanOwner = await pgClient.query(`
    SELECT c.unit_id, c.owner_user_id
      FROM controllers c
     WHERE c.owner_user_id IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = c.owner_user_id)
     ORDER BY c.owner_user_id;
  `);
  if (orphanOwner.rows.length > 0) {
    console.warn(`\n⚠ ${orphanOwner.rows.length} 行 controllers 的 owner_user_id 在 users 里找不到：`);
    console.warn('  unit_id\t\towner_user_id');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanOwner.rows) {
      console.warn(`  ${r.unit_id}\t\t${r.owner_user_id}`);
    }
  }

  if (orphanCompany.rows.length > 0 || orphanOwner.rows.length > 0) {
    throw new Error(
      `Cannot restore FK constraints: found ${orphanCompany.rows.length} orphaned company_id(s) ` +
      `and ${orphanOwner.rows.length} orphaned owner_user_id(s). See details above.`,
    );
  }

  // 恢复外键（ON DELETE SET NULL，与 initDatabase.js 一致）
  await pgClient.query(`
    ALTER TABLE controllers
      ADD CONSTRAINT controllers_owner_user_id_fkey
        FOREIGN KEY (owner_user_id) REFERENCES users(user_id)        ON DELETE SET NULL,
      ADD CONSTRAINT controllers_company_id_fkey
        FOREIGN KEY (company_id)    REFERENCES companies(company_id) ON DELETE SET NULL;
  `);
  console.log('✓ Restored controllers FK constraints');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateControllers(pgClient);
    await pgClient.query('COMMIT');

    console.log('\n✓ migration-controller completed.');
    console.log('Next: node src/migration-assignment.js');
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
