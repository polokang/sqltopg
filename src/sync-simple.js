/**
 * sync-simple.js
 *
 * 常驻增量同步：SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)。
 * 由 index.js 按 SYNC_INTERVAL_MINUTES 调度（方案 A 定为每 2 分钟；0 = 每天定时一次）。
 *
 * 同步的表（snake_case，见 src/initDatabase.js）：
 *   companies                   ← dbo.CompanyInfomation
 *   users                       ← dbo.UserInfomation
 *   controllers                 ← dbo.ControllerInfo（LEFT JOIN UserInfomation 取 billing_email）
 *   user_controller_assignments ← dbo.UserControllerAssignment
 *   controller_service_logs     ← dbo.ControllerNotes（面向客户）
 *   controller_service_tasks    ← dbo.ControllerInfo.Action（面向客户）
 *   controller_subscriptions    ← dbo.ControllerInfo.RenewalInv/RenewalDate（kind='legacy'）
 *   admin_controller_notes      ← dbo.ControllerInfo.Notes（按 '-->' 拆行，**仅内部**）
 *   admin_operation_logs        ← dbo.OperationLog（增量，**仅内部**）
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
 * I2 **users / controllers / user_controller_assignments 的孤儿清理只删 sync_source='mssql' 的行；
 *    UPSERT 命中时把 sync_source 改回 'mssql'。**
 *    Pro 双写写进 PG 的行标 'pro'，保护两类行不被误删：
 *      · 竞态：Pro 在本轮"读快照"之后才写进 SQL Server 的行，不在快照里；
 *      · 只存在于 PG 的授权行（例如给 Manager 存告警偏好，台账 R-008）。
 *    下一轮快照里有了它，UPSERT 就把它"收编"为 'mssql'，此后源库删了它才会被清理。
 *    （2026-10-14 方案 A 阶段 0.2：users / controllers 重新加回此列。）
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
  classifySerials,
  parseNoteDate,
  parseControllerNotes,
  deriveOperationLogRows,
  deriveNoteAuthors,
  utcInstantToBrisbaneDate,
  wallClockToDate,
  CASH_ACCOUNT_USER_ID,
  toBool,
  nullIfBlank,
  deriveCompanyLogo,
  DEFAULT_LOGO,
  wallClockToInstant,
} from './derive.js';
import { getSqlServerConfig, getPostgresUrl } from './dbConfig.js';
import { startRun, finishRun } from './syncLog.js';
import {
  upsertChunked,
  CHUNK_SIZE,
  UPSERT_COMPANIES,
  UPSERT_USERS,
  UPSERT_CONTROLLERS,
  UPSERT_ASSIGNMENTS,
  UPSERT_ADMIN_CONTROLLER_NOTES,
  PRUNE_ADMIN_CONTROLLER_NOTES,
  UPSERT_ADMIN_OPERATION_LOGS,
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
      m.UserId AS ManagerUserId,
      o.Logo AS OwnerLogo, o.Candidates AS OwnerLogoCandidates
    FROM ${SRC_DB}.dbo.CompanyInfomation c
    OUTER APPLY (
      SELECT TOP 1 u.UserId
        FROM ${SRC_DB}.dbo.UserInfomation u
       WHERE u.CompanyId = c.CompanyId
         AND u.UserTypeId = ${USER_TYPE.CORPORATE_MANAGER}
         AND u.Email IS NOT NULL
       ORDER BY u.UserId
    ) m
    -- 公司 logo 优先取 Owner（Production=1）用户的 Logo，见 derive.deriveCompanyLogo。
    -- 多个候选时取最近更新的；与 migration-company.js 的同一段保持一致。
    OUTER APPLY (
      SELECT TOP 1 u.Logo, COUNT(*) OVER () AS Candidates
        FROM ${SRC_DB}.dbo.UserInfomation u
       WHERE u.CompanyId = c.CompanyId
         AND u.Production = 1
         AND NULLIF(LTRIM(RTRIM(u.Logo)), '') IS NOT NULL
         AND u.Logo <> '${DEFAULT_LOGO}'
       ORDER BY COALESCE(u.LastUpdateDate, u.DateCreated) DESC, u.UserId
    ) o
  `);

  const rows = result.recordset;
  console.log(`  Read ${rows.length} source rows`);

  // manager_user_id 的外键指向 users，而同步顺序是 companies 先于 users
  // （controllers.company_id 需要 companies 已在）。所以这里先记下候选值，
  // 等 syncUsers 跑完再由 backfillCompanyManagers 回填。
  const pendingManagers = [];
  const validCompanyIds = new Set();
  const multiOwnerLogo = [];   // 有多个 Owner 候选 logo 的公司，取了最近更新的那个

  const payload = rows.map((row) => {
    if (row.ManagerUserId != null) {
      pendingManagers.push({ companyId: row.CompanyId, managerUserId: row.ManagerUserId });
    }
    validCompanyIds.add(row.CompanyId);
    if (row.OwnerLogoCandidates > 1) multiOwnerLogo.push(row.CompanyId);
    return {
      company_id: row.CompanyId,
      name:       String(row.CompanyName).trim(),
      phone:      nullIfBlank(row.Phone),
      email:      nullIfBlank(row.Email),
      logo_url:   deriveCompanyLogo(row.OwnerLogo, row.CompanyLogoName),
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
  if (multiOwnerLogo.length > 0) {
    console.log(`  ℹ ${multiOwnerLogo.length} 家公司有多个 Owner logo 候选，取最近更新的：${multiOwnerLogo.join(', ')}`);
  }
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
      UserName, Name, PasswordHash, JWCode,
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
    for (const d of dropped) {
      console.warn(`      ${d.email}  保留 UserId=${d.keptUserId}，丢弃 UserId=${d.droppedUserId}`);
    }
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
      // Pro 独有（Q18）：老库 Logo 是公司 logo 不是头像。仅 INSERT 生效（给 NULL），DO UPDATE 不含此列
      avatar_url:          null,
      jw_code:             deriveJwCode(row.JWCode),
      roles:               deriveRoles(row.UserTypeId),
      // 仅 INSERT 时生效 —— 不变式 I3，UPSERT_USERS 的 DO UPDATE 不含此列
      managed_company_ids: deriveManagedCompanyIds(row.UserTypeId, row.CompanyId),
      activated:           toBool(row.Activated),
      mfa_enabled:         toBool(row.RequiresTwoFactorAuth),
      // 墙上时间 → 真时刻；UPSERT 取与 PG 现值较晚者（Q19，Pro 登录只写 PG）
      last_login_at:       wallClockToInstant(row.LastLoginTime),
      created_at:          row.DateCreated,
      updated_at:          row.LastUpdateDate,
    };
  });

  const upserted = await upsertChunked(pgClient, UPSERT_USERS, payload);

  // 孤儿清理：删掉源库里已经不存在的行。只删 'mssql' 行（不变式 I2）。
  const del = await pgClient.query(
    `DELETE FROM users
      WHERE sync_source = 'mssql'
        AND user_id <> ALL($1::int[])`,
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
  const serialNoteUnits = [];   // Serials 被当成备注（不是序列号清单）的设备

  const payload = rows.map((row) => {
    // 外键目标不在 PG 时置 NULL，而不是让整轮同步失败
    let ownerId = row.AccManagerId ?? null;
    if (ownerId != null && !validUserIds.has(ownerId)) { ownerId = null; nulledOwner++; }

    let companyId = row.CompanyId ?? null;
    if (companyId != null && !validCompanyIds.has(companyId)) { companyId = null; nulledCompany++; }

    validUnitIds.add(row.UnitId);

    // Serials 里写的是整句备注而不是序列号清单，整段保留并记下来
    const serials = classifySerials(row.Serials);
    if (serials.isNote) serialNoteUnits.push(row.UnitId);

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
      creator:           nullIfBlank(row.SiteLocation),
      tags:              parseTags(row.Suburb),
      billing_email:     row.BillingEmail || null,
      bar_code:          row.Barcode || null,
      po_number:         row.CustPO || null,
      accessory_serial_numbers: serials.values,
    };
  });

  const upserted = await upsertChunked(pgClient, UPSERT_CONTROLLERS, payload);

  // 只删 'mssql' 行（不变式 I2）。
  const del = await pgClient.query(
    `DELETE FROM controllers
      WHERE sync_source = 'mssql'
        AND unit_id <> ALL($1::int[])`,
    [[...validUnitIds]],
  );
  console.log(
    `  实际写入 ${upserted} / ${rows.length} 行，清理 ${del.rowCount} 条陈旧记录` +
    (nulledOwner || nulledCompany
      ? `（owner 置空 ${nulledOwner}、company 置空 ${nulledCompany}）`
      : ''),
  );
  if (serialNoteUnits.length > 0) {
    console.warn(
      `  ⚠ ${serialNoteUnits.length} 台设备的 Serials 写的是备注而非序列号清单，已整段保留：`);
    console.warn('      ' + serialNoteUnits.join(', '));
  }
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
/**
 * 服务记录：ControllerNotes / ControllerInfo.Action / ControllerInfo.Renewal*
 *
 * ── 为什么这些也要每轮同步 ──────────────────────────────────────────────────
 *
 * 一度以为它们是一次性迁移，其实不是：旧站还在跑，服务工程师每天都在往
 * ControllerNotes 里写（2026-09-16 还有新记录）。只要旧站不停，它们就是活的源，
 * 和 users / controllers 一样要持续镜像。
 *
 * ── 键与冲突处理 ────────────────────────────────────────────────────────────
 *
 * 用 (source, source_id) 作自然键：
 *   controller_service_logs   source_id = ControllerNotes.ID
 *   controller_service_tasks  source_id = UnitId（Action 是覆盖写，每台最多一条）
 *
 * 应用自己新建的记录 source='manual' 且 source_id 为 NULL，落不进这个偏唯一索引，
 * **永远不会被同步动到**。
 */
async function syncServiceRecords(pgClient, mssqlPool) {
  console.log('=== Syncing service records ===');
  const stats = {};

  // ── ControllerNotes → controller_service_logs ──────────────────────────
  const notes = (await mssqlPool.request().query(`
    SELECT ID, UnitId, UserId, Notes, DateCreated
      FROM ${SRC_DB}.dbo.ControllerNotes ORDER BY ID`)).recordset;

  let logsW = 0;
  let logsBlank = 0;
  for (const r of notes) {
    const body = nullIfBlank(r.Notes);
    if (!body) { logsBlank++; continue; }
    const res = await pgClient.query(
      `INSERT INTO controller_service_logs
         (unit_id, body, occurred_on, created_by, created_at, source, source_id)
       VALUES ($1,$2,$3,$4,$5,'controller_notes',$6)
       ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
       DO UPDATE SET unit_id = EXCLUDED.unit_id, body = EXCLUDED.body,
                     occurred_on = EXCLUDED.occurred_on, created_by = EXCLUDED.created_by
       WHERE (controller_service_logs.unit_id, controller_service_logs.body,
              controller_service_logs.occurred_on, controller_service_logs.created_by)
         IS DISTINCT FROM
             (EXCLUDED.unit_id, EXCLUDED.body, EXCLUDED.occurred_on, EXCLUDED.created_by)`,
      [r.UnitId, body, parseNoteDate(body), r.UserId, r.DateCreated, r.ID],
    );
    logsW += res.rowCount;
  }
  console.log(`  controller_service_logs: 实际写入 ${logsW} / ${notes.length - logsBlank} 行${logsBlank ? `（跳过 ${logsBlank} 条空正文）` : ''}`);
  stats.controller_service_logs = { read: notes.length, written: logsW };

  // ── ControllerInfo.Action → controller_service_tasks ───────────────────
  //
  // ⚠️ 只写 body / occurred_on / unit_id。status、assigned_to、completed_* 这些是
  //    **Pro 侧的工作状态**，源库没有对应字段，同步绝不能碰 —— 否则人工把任务标成
  //    完成，下一轮同步就给打回去了。
  const actions = (await mssqlPool.request().query(`
    SELECT UnitId, Action, DateLastUpdate
      FROM ${SRC_DB}.dbo.ControllerInfo
     WHERE LTRIM(RTRIM(ISNULL(Action,''))) <> '' ORDER BY UnitId`)).recordset;

  let taskW = 0;
  for (const r of actions) {
    const body = nullIfBlank(r.Action);
    if (!body) continue;
    const res = await pgClient.query(
      `INSERT INTO controller_service_tasks
         (unit_id, body, status, occurred_on, created_at, source, source_id)
       VALUES ($1,$2,'unknown',$3,$4,'controller_info_action',$5)
       ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
       DO UPDATE SET body = EXCLUDED.body, occurred_on = EXCLUDED.occurred_on
       WHERE (controller_service_tasks.body, controller_service_tasks.occurred_on)
         IS DISTINCT FROM (EXCLUDED.body, EXCLUDED.occurred_on)`,
      [r.UnitId, body, parseNoteDate(body), r.DateLastUpdate ?? new Date(), r.UnitId],
    );
    taskW += res.rowCount;
  }
  console.log(`  controller_service_tasks: 实际写入 ${taskW} / ${actions.length} 行`);
  stats.controller_service_tasks = { read: actions.length, written: taskW };

  // ── ControllerInfo.Renewal* → controller_subscriptions (kind='legacy') ──
  //
  // 老的 SIM 卡续费。源库只有发票号 + 到期日，没有期限/起算日，所以 kind='legacy'
  // 并把那些字段留空（表上的完整性 CHECK 对 legacy 豁免）。
  const renewals = (await mssqlPool.request().query(`
    SELECT UnitId, LTRIM(RTRIM(RenewalInv)) AS RenewalInv, RenewalDate
      FROM ${SRC_DB}.dbo.ControllerInfo
     WHERE RenewalDate IS NOT NULL AND LTRIM(RTRIM(ISNULL(RenewalInv,''))) <> ''
     ORDER BY UnitId`)).recordset;

  let subW = 0;
  for (const r of renewals) {
    const res = await pgClient.query(
      `INSERT INTO controller_subscriptions
         (unit_id, kind, ends_on, invoice_no, comm_types_at_sale,
          paid_by_user_id, note, entered_at)
       SELECT $1,'legacy',$2,$3,c.comm_types,
              -- 付费人取设备 owner；没有就记在公司自己的现金账户名下
              COALESCE(c.owner_user_id, ${CASH_ACCOUNT_USER_ID}),
              '从 ControllerInfo.RenewalInv/RenewalDate 同步；源库无期限与起算日', NOW()
         FROM controllers c WHERE c.unit_id = $1
       ON CONFLICT (unit_id, invoice_no) WHERE invoice_no IS NOT NULL AND voided_at IS NULL
       DO UPDATE SET ends_on = EXCLUDED.ends_on
       WHERE controller_subscriptions.ends_on IS DISTINCT FROM EXCLUDED.ends_on`,
      [r.UnitId, r.RenewalDate, r.RenewalInv],
    );
    subW += res.rowCount;
  }
  console.log(`  controller_subscriptions(legacy): 实际写入 ${subW} / ${renewals.length} 行`);
  stats.controller_subscriptions = { read: renewals.length, written: subW };

  return { stats };
}

/**
 * 内部数据：ControllerInfo.Notes → admin_controller_notes，
 *           OperationLog        → admin_operation_logs
 *
 * ⚠️ 这两张表都是**仅内部可见**的，与上面 syncServiceRecords 写的两张面向客户的
 *    表是不同的东西，别合并。区别只在受众，但那正是最容易出事的地方。
 *
 * 和 syncServiceRecords 里那三段逐行写法不同，这里走 upserts.js 的批量语句 ——
 * 备注拆出 6786 行、审计首轮 10826 行，逐行往返（实测 16.87 ms）要 5 分钟，
 * 塞不进调度周期。
 */
async function syncAdminRecords(pgClient, mssqlPool) {
  console.log('=== Syncing admin-only records ===');
  const stats = {};

  // ── ① 备注作者：先把 OperationLog 里改过 Notes 的记录读出来 ──────────────
  //
  // ControllerInfo.Notes 那一列不记作者，但 adminpage 每次改备注都会在
  // OperationLog 里留下改前改后的全文 —— 把新出现的那一段认出来就知道是谁加的。
  // 规则在 derive.deriveNoteAuthors，覆盖率 16.8%（见那里的说明）。
  //
  // ⚠️ 这里**不能只读增量**：要判断"某一段是谁加的"，得看它第一次出现在哪条日志
  //    里，那可能是两年前。所以按 Notes 过滤、读全量（4287 行，远小于整表 1.6 万）。
  const noteLogs = (await mssqlPool.request().query(`
    SELECT LogId, UserId, OperationTime, OriginalRecord, UpdatedRecord
      FROM ${SRC_DB}.dbo.OperationLog
     WHERE OperationType = 'MODIFY_CONTROLLER'
       AND (OriginalRecord LIKE '%"Notes"%' OR UpdatedRecord LIKE '%"Notes"%')
     ORDER BY LogId`)).recordset;
  const noteAuthors = deriveNoteAuthors(noteLogs);

  // ── ② ControllerInfo.Notes → admin_controller_notes ─────────────────────
  //
  // 源列一格里塞着一台设备的全部历史，段间用 '-->' 分隔。拆分与过滤规则在
  // derive.parseControllerNotes（逐键存盘噪音、字段变更回显、'New Controller'
  // 前缀全部丢掉）。
  //
  // occurred_on 三级回退：日志时间（最准）→ 正文里的澳式日期 → 设备建档日期。
  //
  // ⚠️ **每台设备都要进 prune 清单，包括备注为空的**，否则"备注被整段清空"
  //    这种情况在 PG 里永远删不掉 —— 这类 bug 不会报错，只会让数据慢慢变脏。
  const noteRows = (await mssqlPool.request().query(`
    SELECT UnitId, Notes, DateCreated
      FROM ${SRC_DB}.dbo.ControllerInfo ORDER BY UnitId`)).recordset;

  const notePayload = [];
  const prune = [];
  let attributed = 0;
  for (const r of noteRows) {
    const fragments = parseControllerNotes(r.Notes);
    prune.push({ source_id: r.UnitId, keep: fragments.length });
    fragments.forEach((body, seq) => {
      const author = noteAuthors.get(`${r.UnitId}|${body}`);
      if (author) attributed++;
      notePayload.push({
        unit_id:     r.UnitId,
        body,
        // 日志时间 → 正文日期 → 设备建档日期
        // ⚠️ 两个源列的时区约定不同：OperationTime 是真 UTC，
        // DateCreated 是布里斯班墙上时间。见 derive.js 那两个函数的说明。
        occurred_on: utcInstantToBrisbaneDate(author?.at)
                     ?? parseNoteDate(body)
                     ?? wallClockToDate(r.DateCreated),
        created_by:  author?.userId ?? null,
        created_at:  r.DateCreated,
        source_id:   r.UnitId,
        source_seq:  seq,
      });
    });
  }

  const notesW = await upsertChunked(pgClient, UPSERT_ADMIN_CONTROLLER_NOTES, notePayload);
  let notesD = 0;
  for (let i = 0; i < prune.length; i += CHUNK_SIZE) {
    const res = await pgClient.query(
      PRUNE_ADMIN_CONTROLLER_NOTES, [JSON.stringify(prune.slice(i, i + CHUNK_SIZE))]);
    notesD += res.rowCount;
  }
  const pct = notePayload.length ? (attributed / notePayload.length * 100).toFixed(1) : '0.0';
  console.log(
    `  admin_controller_notes: ${notePayload.length} 段（来自 ${countUnits(notePayload)} 台）`
    + `，其中 ${attributed} 段能归属到人（${pct}%）`
    + `，实际写入 ${notesW} 行，删除 ${notesD} 行`);
  stats.admin_controller_notes = {
    read: notePayload.length, written: notesW, deleted: notesD, attributed,
  };

  // ── ③ OperationLog → admin_operation_logs ───────────────────────────────
  //
  // 摊平成字段级改动：一条源日志改了 N 个字段就是 N 行。迁移口径、两种源格式的
  // 处理、密码脱敏都在 derive.deriveOperationLogRows，那里是唯一一份规则。
  //
  // ⚠️ **增量读，不是全量。** 源表是 identity 主键的追加型日志（1.6 万行且每天
  //    还在长），旧行不会被改。只取 LogId 大于已同步最大值的部分，同步耗时就与
  //    新增量成正比而不是与表大小成正比。库是空的时候 max = 0，自然退化成全量。
  const { rows: [{ max_id: maxId }] } = await pgClient.query(
    `SELECT COALESCE(MAX(source_id), 0) AS max_id
       FROM admin_operation_logs WHERE source = 'operation_log'`);

  const opRows = (await mssqlPool.request().query(`
    SELECT LogId, UserId, OperationType, OriginalRecord, UpdatedRecord, OperationTime
      FROM ${SRC_DB}.dbo.OperationLog
     WHERE LogId > ${Number(maxId)}
     ORDER BY LogId`)).recordset;

  const opPayload = [];
  let skippedLogs = 0;
  for (const r of opRows) {
    const derived = deriveOperationLogRows(r);
    if (derived.length === 0) { skippedLogs++; continue; }
    opPayload.push(...derived);
  }

  const opsW = await upsertChunked(pgClient, UPSERT_ADMIN_OPERATION_LOGS, opPayload);
  console.log(
    `  admin_operation_logs: 源库新增 ${opRows.length} 条日志（LogId > ${Number(maxId)}）`
    + `，跳过 ${skippedLogs} 条（登录/新增/配件等不迁的类型）`
    + `，摊平成 ${opPayload.length} 行字段改动，实际写入 ${opsW} 行`);
  stats.admin_operation_logs = {
    read: opRows.length, written: opsW, fieldRows: opPayload.length,
  };

  return { stats };
}

/** 日志里那句"来自 N 台"—— 只为打印好看，没有别的用处 */
function countUnits(payload) {
  return new Set(payload.map((r) => r.unit_id)).size;
}


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

    // 放最后：这三张表引用 controllers / users，要等上面都就位
    const sr = await syncServiceRecords(pgClient, mssqlPool);
    Object.assign(tables, sr.stats);

    const ar = await syncAdminRecords(pgClient, mssqlPool);
    Object.assign(tables, ar.stats);

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
