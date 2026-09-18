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
 * I2 **只有 user_controller_assignments 上还有 sync_source，孤儿清理只删该表的 'mssql' 行。**
 *    users / controllers 已去掉这一列 —— 两张表整表来自 SQL Server，没有 Pro 自建行。
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
  deriveCommType,
  deriveControllerModel,
  toBool,
  nullIfBlank,
} from './derive.js';
import { getSqlServerConfig, getPostgresUrl } from './dbConfig.js';
import { startRun, finishRun } from './syncLog.js';
import {
  upsertChunked,
  UPSERT_COMPANIES,
  UPSERT_USERS,
  UPSERT_CONTROLLERS,
  UPSERT_ASSIGNMENTS,
} from './upserts.js';

dotenv.config();

// ─── PostgreSQL connection ────────────────────────────────────────────────────

const pgPool = new Pool({
  connectionString: getPostgresUrl(),
  ssl: { rejectUnauthorized: false },
});

// ─── SQL Server connection ────────────────────────────────────────────────────

const { cfg: sqlConfig, srcDb: SRC_DB } = getSqlServerConfig();

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

  const payload = rows.map((row) => {
    if (row.ManagerUserId != null) {
      pendingManagers.push({ companyId: row.CompanyId, managerUserId: row.ManagerUserId });
    }
    validCompanyIds.add(row.CompanyId);
    return {
      company_id: row.CompanyId,
      name:       String(row.CompanyName).trim(),
      phone:      nullIfBlank(row.Phone),
      email:      nullIfBlank(row.Email),
      logo_url:   nullIfBlank(row.CompanyLogoName) ?? 'YourLogoGoesToHere.JPG',
      activated:  toBool(row.Activated),
      created_at: row.DateCreated,
      updated_at: row.LastUpdateDate,
    };
  });

  const upserted = await upsertChunked(pgClient, UPSERT_COMPANIES, payload);

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

  const payload = rows.map((row) => {
    validUserIds.add(row.UserId);
    return {
      user_id:             row.UserId,
      company_id:          row.CompanyId,
      email:               row.Email,
      email_verified:      toBool(row.Activated),
      mobile:              deriveMobile(row.Mobile),
      mobile_verified:     toBool(row.IsPhoneVerified),
      name:                deriveUserName(row),
      password_hash:       row.PasswordHash || '',
      avatar_url:          row.Logo || null,
      jw_code:             deriveJwCode(row.JWCode),
      roles:               deriveRoles(row.UserTypeId),
      // 仅 INSERT 时生效 —— 不变式 I3，UPSERT_USERS 的 DO UPDATE 不含此列
      managed_company_ids: deriveManagedCompanyIds(row.UserTypeId, row.CompanyId),
      activated:           toBool(row.Activated),
      mfa_enabled:         toBool(row.RequiresTwoFactorAuth),
      last_login_at:       row.LastLoginTime || null,
      created_at:          row.DateCreated,
      updated_at:          row.LastUpdateDate,
    };
  });

  const upserted = await upsertChunked(pgClient, UPSERT_USERS, payload);

  // 孤儿清理：删掉源库里已经不存在的行。
  // users 上不再有 sync_source —— 这张表整表来自 SQL Server，没有 Pro 自建的行。
  const del = await pgClient.query(
    `DELETE FROM users
      WHERE user_id <> ALL($1::int[])`,
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
  let nulledOwner = 0;
  let nulledCompany = 0;

  const payload = rows.map((row) => {
    // 外键目标不在 PG 时置 NULL，而不是让整轮同步失败
    let ownerId = row.AccManagerId ?? null;
    if (ownerId != null && !validUserIds.has(ownerId)) { ownerId = null; nulledOwner++; }

    let companyId = row.CompanyId ?? null;
    if (companyId != null && !validCompanyIds.has(companyId)) { companyId = null; nulledCompany++; }

    validUnitIds.add(row.UnitId);

    // SIMCardNo 一列拆三列：通信方式 / 真卡号 / 原始串（见 derive.js）
    const comm = deriveCommType(row.SIMCardNo);

    return {
      unit_id:           row.UnitId,
      serial_no:         row.SerialNo || null,
      sim_card_no:       comm.simCardNo,
      comm_types:        comm.commTypes,
      owner_user_id:     ownerId,
      company_id:        companyId,
      system_id:         row.SystemID,
      system_type:       nullIfBlank(row.FirmwareVersion),
      model_code:        deriveControllerModel(row.ControllerModel),
      timezone_id:       row.LinuxTimeZoneId || null,
      production_status: row.ProductionStatus ?? null,
      activated:         toBool(row.Activated),
      created_at:        row.DateCreated,
      updated_at:        row.DateLastUpdate,
      site_location:     row.SiteLocation || null,
      tags:              parseTags(row.Suburb),
      billing_email:     row.BillingEmail || null,
      bar_code:          row.Barcode || null,
      po_number:         row.CustPO || null,
      spare_parts:       row.Serials || null,
      notes:             row.Notes || null,
    };
  });

  const upserted = await upsertChunked(pgClient, UPSERT_CONTROLLERS, payload);

  // controllers 同样不再有 sync_source（理由见 users 处）。
  const del = await pgClient.query(
    `DELETE FROM controllers
      WHERE unit_id <> ALL($1::int[])`,
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

  const keepUserIds = rows.map((r) => r.UserId);
  const keepUnitIds = rows.map((r) => r.UnitId);

  const payload = rows.map((row) => ({
    user_id:          row.UserId,
    unit_id:          row.UnitId,
    permission:       row.permission,
    send_sms_alarm:   row.sendSmsAlarm,
    send_email_alarm: row.sendEmailAlarm,
  }));

  const upserted = await upsertChunked(pgClient, UPSERT_ASSIGNMENTS, payload);

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
