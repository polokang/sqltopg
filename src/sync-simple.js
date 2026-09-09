/**
 * sync-simple.js
 *
 * 常驻增量同步：SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)。
 * 由 index.js 每天布里斯班时间 00:00 调用一次（2026-09-09 前是每 2 分钟）。
 *
 * 同步的表（snake_case，见 src/initDatabase.js）：
 *   companies                   ← dbo.CompanyInfomation
 *   users                       ← dbo.UserInfomation
 *   controllers                 ← dbo.ControllerInfo（LEFT JOIN UserInfomation 取 billing_email）
 *   user_controller_assignments ← dbo.UserControllerAssignment
 *
 * 不同步（Pro 自有数据，SQL Server 没有对应表/字段）：
 *   user_identities            Passkey / Google 绑定
 *   user_mfa_factors           MFA
 *   users.managed_company_ids  ⚠️ 见下方 I3
 *
 * 策略：全量读源库 → 按主键 UPSERT → 删掉源库已消失的行（孤儿清理）。
 * 四张表在**同一个事务**里完成。
 *
 * ── 三条不变式 ───────────────────────────────────────────────────────────────
 *
 * I1 **全部派生规则来自 src/derive.js。**
 *    一次性迁移（migration-*.js）和本脚本必须产出完全一致的结果，否则数据会每
 *    2 分钟翻来翻去、且极难排查。规则只允许写在 derive.js 里，两边 import 同一个
 *    函数 —— 历史上这两边的邮箱去重逻辑就已经漂过（sync 保留一行，migration 中止）。
 *
 * I2 **孤儿清理只删 sync_source = 'mssql' 的行。**
 *    双写期 Pro 会往 PG 写 sync_source='pro' 的行（例如给 Manager 存告警偏好），
 *    这些行在 SQL Server 里没有对应记录，无条件清理会把它们静默删掉。（台账 R-008）
 *
 * I3 **users.managed_company_ids 永不被覆盖。**
 *    SQL Server 没有这个字段。初值由 derive.deriveManagedCompanyIds 给
 *    （CorporateManager → [本人公司]），之后只由 Pro 的界面维护 —— David 邮件里
 *    Nick Duncan / Jay Dean 要管 HydroChem 多家分公司，只能人工配成
 *    [2002, 2008, 2034, ...]。所以它出现在 INSERT 的列里（新用户要有初值），
 *    但**刻意不出现在 ON CONFLICT DO UPDATE SET 里**。
 *
 * ── 2026-09-09 的语义变更（依据 David Grevell 的权限模型邮件）────────────────
 *
 *   · roles 由 UserTypeId 派生，不再读 Production，不再有 'owner'
 *     （旧规则把 243 个 Owner 全变成 Manager，导致同一 CompanyId 下的客户互相可见）
 *   · controllers.company_id 取设备自己的 CompanyId，不再取 Owner 所属公司（R-003）
 *   · users.jw_code 新增；companies 不再有 jw_code
 *   · companies 纳入同步（原先只有一次性迁移，老站新增公司后 PG 看不到 —— R-006）
 *   · 授权表按 (user_id, unit_id) 冲突，不再把 SeqKey 写进 id（R-007）
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { fileURLToPath } from 'url';
import { resolve } from 'path';
import {
  USER_TYPE,
  deriveRoles,
  deriveManagedCompanyIds,
  deriveJwCode,
  deriveUserName,
  deriveMobile,
  dedupeUsersByEmail,
  mergeAssignmentsByPair,
  parseTags,
  toBool,
  nullIfBlank,
} from './derive.js';
import { startRun, finishRun } from './syncLog.js';

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

// 源库名从连接串的 database= 取。**不要硬编码** —— 三段式表名写死库名的话，
// 改 .env 指向别的库（如 AquariusDev2）根本不生效，而且不会报错。
const SRC_DB = sqlConfig.database || 'AquariusEmailDB';

// ─── companies ────────────────────────────────────────────────────────────────

async function syncCompanies(pgClient, mssqlPool) {
  console.log('\n=== Syncing companies ===');

  // manager_user_id 取该公司 UserTypeId=24 里 UserId 最小的那个 ——
  // 与 migration-company.js 的同一段查询保持一致，结果确定、可重复。
  const result = await mssqlPool.request().query(`
    SELECT
      c.CompanyId, c.CompanyName, c.Phone, c.Email, c.CompanyLogoName,
      c.Activated, c.DateCreated,
      COALESCE(c.LastUpdateDate, c.DateCreated) AS LastUpdateDate,
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
  `);

  const rows = result.recordset;
  console.log(`  Read ${rows.length} source rows`);

  // manager_user_id 的外键指向 users，而同步顺序是 companies 先于 users
  // （controllers.company_id 需要 companies 已在）。所以这里先记下候选值，
  // 等 syncUsers 跑完再由 backfillCompanyManagers 回填。
  const pendingManagers = [];
  const validCompanyIds = new Set();
  let upserted = 0;

  for (const row of rows) {
    const res = await pgClient.query(
      `INSERT INTO companies
         (company_id, name, phone, email, logo_url, activated, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (company_id) DO UPDATE SET
         name       = EXCLUDED.name,
         phone      = EXCLUDED.phone,
         email      = EXCLUDED.email,
         logo_url   = EXCLUDED.logo_url,
         activated  = EXCLUDED.activated,
         updated_at = EXCLUDED.updated_at
       WHERE (companies.name, companies.phone, companies.email,
              companies.logo_url, companies.activated, companies.updated_at)
         IS DISTINCT FROM
             (EXCLUDED.name, EXCLUDED.phone, EXCLUDED.email,
              EXCLUDED.logo_url, EXCLUDED.activated, EXCLUDED.updated_at)`,
      [
        row.CompanyId,
        String(row.CompanyName).trim(),
        nullIfBlank(row.Phone),
        nullIfBlank(row.Email),
        nullIfBlank(row.CompanyLogoName) ?? 'YourLogoGoesToHere.JPG',
        toBool(row.Activated),
        row.DateCreated,
        row.LastUpdateDate,
      ],
    );
    if (row.ManagerUserId != null) {
      pendingManagers.push({ companyId: row.CompanyId, managerUserId: row.ManagerUserId });
    }
    validCompanyIds.add(row.CompanyId);
    upserted += res.rowCount;
  }

  // ⚠️ companies 刻意**不做孤儿清理**：company_id 是 SERIAL，Pro 侧可能建过公司；
  //    而且删公司会经 controllers.company_id 的 ON DELETE SET NULL 让整批设备失去
  //    归属（也就失去全部 Manager 可见性），代价远大于收益。老站删公司走人工处理。
  console.log(`  实际写入 ${upserted} / ${rows.length} 行（未变化的行不写；companies 不做孤儿清理）`);
  return {
    validCompanyIds,
    pendingManagers,
    stats: { read: rows.length, written: upserted, deleted: 0 },
  };
}

/** 在 syncUsers 之后回填 companies.manager_user_id（外键要求 users 先在）。 */
async function backfillCompanyManagers(pgClient, pendingManagers, validUserIds) {
  let updated = 0;
  let skipped = 0;
  for (const { companyId, managerUserId } of pendingManagers) {
    if (!validUserIds.has(managerUserId)) { skipped++; continue; }
    const res = await pgClient.query(
      `UPDATE companies SET manager_user_id = $2
        WHERE company_id = $1 AND manager_user_id IS DISTINCT FROM $2`,
      [companyId, managerUserId],
    );
    updated += res.rowCount;
  }
  console.log(
    `  companies.manager_user_id: 更新 ${updated} 家` +
    (skipped ? `，跳过 ${skipped} 家（经理不在 users 里）` : ''),
  );
}

// ─── users ────────────────────────────────────────────────────────────────────

async function syncUsers(pgClient, mssqlPool) {
  console.log('\n=== Syncing users ===');

  const result = await mssqlPool.request().query(`
    SELECT
      UserId, CompanyId, UserTypeId, Mobile, IsPhoneVerified, Email,
      UserName, Name, PasswordHash, Logo, JWCode,
      Activated, RequiresTwoFactorAuth,
      LastLoginTime, DateCreated,
      COALESCE(LastUpdateDate, DateCreated) AS LastUpdateDate
    FROM ${SRC_DB}.dbo.UserInfomation
    WHERE Email IS NOT NULL
  `);

  const { rows, dropped } = dedupeUsersByEmail(result.recordset);
  console.log(`  Read ${result.recordset.length} source rows → ${rows.length} after email dedup`);
  if (dropped.length > 0) {
    console.warn(`  ⚠ ${dropped.length} 组重复邮箱已去重（保留 Activated > CorporateManager > 较大 UserId）`);
  }

  const validUserIds = new Set();
  let upserted = 0;

  for (const row of rows) {
    const res = await pgClient.query(
      `INSERT INTO users (
         user_id, company_id, email, email_verified, mobile, mobile_verified,
         name, password_hash, avatar_url, jw_code,
         roles, managed_company_ids, activated, mfa_enabled,
         last_login_at, created_at, updated_at
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
       ON CONFLICT (user_id) DO UPDATE SET
         company_id      = EXCLUDED.company_id,
         email           = EXCLUDED.email,
         email_verified  = EXCLUDED.email_verified,
         mobile          = EXCLUDED.mobile,
         mobile_verified = EXCLUDED.mobile_verified,
         name            = EXCLUDED.name,
         password_hash   = EXCLUDED.password_hash,
         avatar_url      = EXCLUDED.avatar_url,
         jw_code         = EXCLUDED.jw_code,
         roles           = EXCLUDED.roles,
         activated       = EXCLUDED.activated,
         mfa_enabled     = EXCLUDED.mfa_enabled,
         last_login_at   = EXCLUDED.last_login_at,
         updated_at      = EXCLUDED.updated_at
       WHERE (users.company_id, users.email, users.email_verified, users.mobile,
              users.mobile_verified, users.name, users.password_hash, users.avatar_url,
              users.jw_code, users.roles, users.activated, users.mfa_enabled,
              users.last_login_at, users.updated_at)
         IS DISTINCT FROM
             (EXCLUDED.company_id, EXCLUDED.email, EXCLUDED.email_verified, EXCLUDED.mobile,
              EXCLUDED.mobile_verified, EXCLUDED.name, EXCLUDED.password_hash, EXCLUDED.avatar_url,
              EXCLUDED.jw_code, EXCLUDED.roles, EXCLUDED.activated, EXCLUDED.mfa_enabled,
              EXCLUDED.last_login_at, EXCLUDED.updated_at)`,
      //  ↑ DO UPDATE 刻意不含 managed_company_ids —— 不变式 I3。SQL Server 没有这个
      //    字段，它由 Pro 人工维护，覆盖会把跨公司的 Manager 范围冲掉。
      //
      //  ↑ WHERE 子句让**没有变化的行完全不被写**：否则每轮同步都会重写全部 5000 行，
      //    触发器把 updated_at 打成 NOW()，同步就不幂等了（WAL/表膨胀同理）。
      [
        row.UserId,
        row.CompanyId,
        row.Email,
        toBool(row.Activated),                    // email_verified
        deriveMobile(row.Mobile),
        toBool(row.IsPhoneVerified),              // mobile_verified
        deriveUserName(row),
        row.PasswordHash || '',
        row.Logo || null,                         // avatar_url
        deriveJwCode(row.JWCode),
        deriveRoles(row.UserTypeId),
        deriveManagedCompanyIds(row.UserTypeId, row.CompanyId), // 仅 INSERT 时生效
        toBool(row.Activated),
        toBool(row.RequiresTwoFactorAuth),        // mfa_enabled
        row.LastLoginTime || null,
        row.DateCreated,
        row.LastUpdateDate,
      ],
    );
    validUserIds.add(row.UserId);
    upserted += res.rowCount;
  }

  // 孤儿清理：只删来自 SQL Server 的行（不变式 I2）
  const del = await pgClient.query(
    `DELETE FROM users
      WHERE sync_source = 'mssql' AND user_id <> ALL($1::int[])`,
    [[...validUserIds]],
  );
  console.log(`  实际写入 ${upserted} / ${rows.length} 行，清理 ${del.rowCount} 条陈旧记录`);
  return {
    validUserIds,
    stats: { read: rows.length, written: upserted, deleted: del.rowCount },
  };
}

// ─── controllers ──────────────────────────────────────────────────────────────

async function syncControllers(pgClient, mssqlPool, validUserIds, validCompanyIds) {
  console.log('\n=== Syncing controllers ===');

  const result = await mssqlPool.request().query(`
    SELECT
      c.UnitId, c.SerialNo, c.SIMCardNo, c.AccManagerId,
      c.CompanyId,                                  -- 设备自己的公司（不是 Owner 的，R-003）
      c.SystemID, c.FirmwareVersion, c.ControllerModel, c.LinuxTimeZoneId,
      c.ProductionStatus, c.Activated,
      c.DateCreated,
      COALESCE(c.DateLastUpdate, c.DateCreated) AS DateLastUpdate,
      c.SiteLocation, c.Suburb,
      u.Email AS BillingEmail,                      -- 派生值，源库无此列（R-004）
      c.Barcode, c.CustPO, c.Serials, c.Notes
    FROM ${SRC_DB}.dbo.ControllerInfo     c
    LEFT JOIN ${SRC_DB}.dbo.UserInfomation u
           ON u.UserId = c.AccManagerId
  `);

  const rows = result.recordset;
  console.log(`  Read ${rows.length} source rows`);

  const validUnitIds = new Set();
  let upserted = 0;
  let nulledOwner = 0;
  let nulledCompany = 0;

  for (const row of rows) {
    // 外键目标不在 PG 时置 NULL，而不是让整轮同步失败
    let ownerId = row.AccManagerId ?? null;
    if (ownerId != null && !validUserIds.has(ownerId)) { ownerId = null; nulledOwner++; }

    let companyId = row.CompanyId ?? null;
    if (companyId != null && !validCompanyIds.has(companyId)) { companyId = null; nulledCompany++; }

    const res = await pgClient.query(
      `INSERT INTO controllers (
         unit_id, serial_no, sim_card_no, owner_user_id, company_id,
         system_id, firmware_version, model_type, timezone_id, production_status,
         activated, created_at, updated_at, site_location, tags, billing_email,
         bar_code, po_number, spare_parts, notes
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
       ON CONFLICT (unit_id) DO UPDATE SET
         serial_no         = EXCLUDED.serial_no,
         sim_card_no       = EXCLUDED.sim_card_no,
         owner_user_id     = EXCLUDED.owner_user_id,
         company_id        = EXCLUDED.company_id,
         system_id         = EXCLUDED.system_id,
         firmware_version  = EXCLUDED.firmware_version,
         model_type        = EXCLUDED.model_type,
         timezone_id       = EXCLUDED.timezone_id,
         production_status = EXCLUDED.production_status,
         activated         = EXCLUDED.activated,
         updated_at        = EXCLUDED.updated_at,
         site_location     = EXCLUDED.site_location,
         tags              = EXCLUDED.tags,
         billing_email     = EXCLUDED.billing_email,
         bar_code          = EXCLUDED.bar_code,
         po_number         = EXCLUDED.po_number,
         spare_parts       = EXCLUDED.spare_parts,
         notes             = EXCLUDED.notes
       WHERE (controllers.serial_no, controllers.sim_card_no, controllers.owner_user_id,
              controllers.company_id, controllers.system_id, controllers.firmware_version,
              controllers.model_type, controllers.timezone_id, controllers.production_status,
              controllers.activated, controllers.updated_at, controllers.site_location,
              controllers.tags, controllers.billing_email, controllers.bar_code,
              controllers.po_number, controllers.spare_parts, controllers.notes)
         IS DISTINCT FROM
             (EXCLUDED.serial_no, EXCLUDED.sim_card_no, EXCLUDED.owner_user_id,
              EXCLUDED.company_id, EXCLUDED.system_id, EXCLUDED.firmware_version,
              EXCLUDED.model_type, EXCLUDED.timezone_id, EXCLUDED.production_status,
              EXCLUDED.activated, EXCLUDED.updated_at, EXCLUDED.site_location,
              EXCLUDED.tags, EXCLUDED.billing_email, EXCLUDED.bar_code,
              EXCLUDED.po_number, EXCLUDED.spare_parts, EXCLUDED.notes)`,
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
        row.DateCreated,
        row.DateLastUpdate,
        row.SiteLocation || null,
        parseTags(row.Suburb),
        row.BillingEmail || null,
        row.Barcode || null,
        row.CustPO || null,
        row.Serials || null,
        row.Notes || null,
      ],
    );
    validUnitIds.add(row.UnitId);
    upserted += res.rowCount;
  }

  const del = await pgClient.query(
    `DELETE FROM controllers
      WHERE sync_source = 'mssql' AND unit_id <> ALL($1::int[])`,
    [[...validUnitIds]],
  );
  console.log(
    `  实际写入 ${upserted} / ${rows.length} 行，清理 ${del.rowCount} 条陈旧记录` +
    (nulledOwner || nulledCompany
      ? `（owner 置空 ${nulledOwner}、company 置空 ${nulledCompany}）`
      : ''),
  );
  return {
    validUnitIds,
    stats: {
      read: rows.length, written: upserted, deleted: del.rowCount,
      nulledOwner, nulledCompany,
    },
  };
}

// ─── user_controller_assignments ──────────────────────────────────────────────

async function syncAssignments(pgClient, mssqlPool, validUserIds, validUnitIds) {
  console.log('\n=== Syncing user_controller_assignments ===');

  const result = await mssqlPool.request().query(`
    SELECT UserId, UnitId, AccessLevel, SendSMSAlarm, SendEmailAlarm
      FROM ${SRC_DB}.dbo.UserControllerAssignment
  `);

  const { rows: allRows, merged } = mergeAssignmentsByPair(result.recordset);

  // 丢掉指向不存在用户/设备的行 —— 外键会拒绝
  const rows = allRows.filter((r) => validUserIds.has(r.UserId) && validUnitIds.has(r.UnitId));
  const skippedFk = allRows.length - rows.length;
  console.log(
    `  Read ${result.recordset.length} source rows → ${rows.length} 行` +
    `（合并 ${merged}、外键无效跳过 ${skippedFk}）`,
  );

  const keepUserIds = [];
  const keepUnitIds = [];
  let upserted = 0;

  for (const row of rows) {
    const res = await pgClient.query(
      `INSERT INTO user_controller_assignments
         (user_id, unit_id, permission, send_sms_alarm, send_email_alarm)
       VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (user_id, unit_id) DO UPDATE SET
         permission       = EXCLUDED.permission,
         send_sms_alarm   = EXCLUDED.send_sms_alarm,
         send_email_alarm = EXCLUDED.send_email_alarm
       WHERE (user_controller_assignments.permission,
              user_controller_assignments.send_sms_alarm,
              user_controller_assignments.send_email_alarm)
         IS DISTINCT FROM
             (EXCLUDED.permission, EXCLUDED.send_sms_alarm, EXCLUDED.send_email_alarm)`,
      [row.UserId, row.UnitId, row.permission, row.sendSmsAlarm, row.sendEmailAlarm],
    );
    keepUserIds.push(row.UserId);
    keepUnitIds.push(row.UnitId);
    upserted += res.rowCount;
  }

  // 孤儿清理按**自然键**比对（不再按 id —— id 已交给 SERIAL，见 R-007），
  // 且只删来自 SQL Server 的行（不变式 I2）。
  const del = await pgClient.query(
    `DELETE FROM user_controller_assignments a
      WHERE a.sync_source = 'mssql'
        AND NOT EXISTS (
          SELECT 1 FROM unnest($1::int[], $2::int[]) AS s(user_id, unit_id)
           WHERE s.user_id = a.user_id AND s.unit_id = a.unit_id
        )`,
    [keepUserIds, keepUnitIds],
  );
  console.log(`  实际写入 ${upserted} / ${rows.length} 行，清理 ${del.rowCount} 条陈旧记录`);
  return {
    stats: {
      read: rows.length, written: upserted, deleted: del.rowCount,
      merged, skippedFk,
    },
  };
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

/**
 * 同步入口，由 index.js 调用。
 *
 * 每次运行都会在 PG 的 sync_runs 表里留一条记录（见 src/syncLog.js）。
 * ⚠️ 日志用**独立连接**写、在本函数的事务之外提交 —— 否则同步失败回滚时会把
 *    自己那条失败记录一起回滚掉，失败就永远不留痕。
 *
 * @param {boolean} closePool     同步完关闭 PG 连接池（独立跑本脚本时用；
 *                                index.js 的常驻定时任务不要关）。
 * @param {object}  [opts]
 * @param {'scheduled'|'startup'|'manual'} [opts.triggerSource] 记进日志的触发来源
 */
async function syncAllTables(closePool = false, { triggerSource = 'manual' } = {}) {
  // 先起日志（独立连接），再开事务
  const runId = await startRun(pgPool, { triggerSource, sourceDb: SRC_DB });

  const pgClient = await pgPool.connect();
  let mssqlPool;
  const tables = {};

  try {
    console.log('='.repeat(60));
    console.log(`Sync run starting… (sync_runs.id=${runId ?? 'n/a'}, trigger=${triggerSource})`);
    console.log('='.repeat(60));

    mssqlPool = await sql.connect(sqlConfig);
    console.log(`✓ Connected to SQL Server: ${sqlConfig.server}/${sqlConfig.database}`);

    await pgClient.query(`SELECT 1`);
    console.log('✓ Connected to PostgreSQL');

    await pgClient.query('BEGIN');

    // 顺序由外键决定：companies → users → controllers → assignments。
    // companies.manager_user_id 反过来依赖 users，所以拆成两步，回填放最后。
    const co = await syncCompanies(pgClient, mssqlPool);
    tables.companies = co.stats;

    const us = await syncUsers(pgClient, mssqlPool);
    tables.users = us.stats;

    const ct = await syncControllers(pgClient, mssqlPool, us.validUserIds, co.validCompanyIds);
    tables.controllers = ct.stats;

    const as = await syncAssignments(pgClient, mssqlPool, us.validUserIds, ct.validUnitIds);
    tables.user_controller_assignments = as.stats;

    await backfillCompanyManagers(pgClient, co.pendingManagers, us.validUserIds);

    await pgClient.query('COMMIT');

    await finishRun(pgPool, runId, { status: 'success', tables });

    console.log('\n' + '='.repeat(60));
    console.log('✓ Sync run completed');
    console.log('='.repeat(60));
  } catch (err) {
    await pgClient.query('ROLLBACK').catch(() => {});
    // 事务已回滚，日志走独立连接，所以这条失败记录能留下来
    await finishRun(pgPool, runId, { status: 'failed', tables, error: err });
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
export { pgPool };

// ─── Standalone entry ─────────────────────────────────────────────────────────

const __filename = fileURLToPath(import.meta.url);
const isMainModule = process.argv[1] && resolve(process.argv[1]) === resolve(__filename);

if (isMainModule) {
  syncAllTables(true, { triggerSource: 'manual' }).catch(() => process.exit(1));
}
