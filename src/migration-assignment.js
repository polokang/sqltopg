/**
 * migration-assignment.js
 *
 * Migrate user_controller_assignments: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : AquariusEmailDB.dbo.UserControllerAssignment
 * Target : PostgreSQL public.user_controller_assignments
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md（snake_case）
 * 目标 schema：src/initDatabase.js
 *
 * Column mapping:
 *   UserId         → user_id
 *   UnitId         → unit_id
 *   AccessLevel    → permission        (0 → 0 READONLY，其余 → 50 NORMAL)
 *   SendSMSAlarm   → send_sms_alarm
 *   SendEmailAlarm → send_email_alarm
 *   SeqKey         → **不迁移**        (见下方 O6)
 *   —              → sync_source       (建表 DEFAULT 'mssql'，不显式写)
 *
 * permission 分级（见 initDatabase.js 的 CHECK）：
 *     0   READONLY
 *    50   NORMAL   (默认)
 *   100   OWNER
 *
 * ── 本次改造的两处行为变化（都来自已定稿的决策，不是临时发挥）─────────────────
 *
 * ① O6 / 台账 R-007：**不再把 SeqKey 写进 id**。
 *    id 完全由 PG 的 SERIAL 管理，`(user_id, unit_id)` 才是跨库对齐的键。
 *    旧脚本直填 SeqKey 会导致双跑期两边撞号（PG 序列与 SQL Server 的 IDENTITY
 *    各自增长），下一轮同步的 ON CONFLICT (id) 会把 Pro 写的行改成另一条记录。
 *    因此也不再需要 setval 重置序列。
 *
 * ② O8 / 台账 R-009：**按 (user_id, unit_id) 去重**，实现在 src/derive.js 的
 *    mergeAssignmentsByPair —— sync-simple.js 用的是同一个函数，两边不会漂。
 *    目标表有 UNIQUE (user_id, unit_id) 而源表没有唯一约束，不去重会撞约束。
 *    合并策略：permission 取最高、send_*_alarm 取 OR。
 *    （旧同步脚本是"取 SeqKey 最大的一条"，会静默丢掉其它行的告警偏好。）
 *    2026-09-08 核实生产数据 18,666 行恰好是 18,666 个唯一组合，当前无重复。
 *
 * Run:
 *   node src/migration-assignment.js
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { startProgress } from './progress.js';
import { mergeAssignmentsByPair } from './derive.js';
import { getSqlServerConfig, getPostgresUrl } from './dbConfig.js';

dotenv.config();

// ─── PostgreSQL connection ────────────────────────────────────────────────────

const pgPool = new Pool({
  connectionString: getPostgresUrl(),
  ssl: { rejectUnauthorized: false },
});

// ─── SQL Server connection ────────────────────────────────────────────────────

const { cfg: sqlConfig, srcDb: SRC_DB } = getSqlServerConfig();

// ─── Migration ────────────────────────────────────────────────────────────────

async function migrateAssignments(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  // 取原始行，去重交给 derive.mergeAssignmentsByPair —— 与 sync-simple.js 共用同一份实现
  const result = await sqlPool.request().query(`
    SELECT [UserId], [UnitId], [AccessLevel], [SendSMSAlarm], [SendEmailAlarm]
      FROM ${SRC_DB}.dbo.UserControllerAssignment
  `);

  const sourceRows = result.recordset;
  await sqlPool.close();

  const { rows, merged } = mergeAssignmentsByPair(sourceRows);
  console.log(`✓ Read ${sourceRows.length} source rows → ${rows.length} 组唯一 (UserId, UnitId)`);
  if (merged > 0) {
    console.warn(`⚠ 合并了 ${merged} 行重复的 (UserId, UnitId)：permission 取最高、告警偏好取 OR`);
    console.warn('  策略依据 docs/migration/03 的 O8，建议在 SQL Server 上核对这些行。');
  } else {
    console.log('✓ 源表没有重复的 (UserId, UnitId)，无需合并');
  }

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  // 先 drop 外键：源表里可能有指向已被丢弃用户 / 不存在设备的行。
  // 插完后诊断孤儿，再 restore。
  await pgClient.query(`
    ALTER TABLE user_controller_assignments
      DROP CONSTRAINT IF EXISTS user_controller_assignments_user_id_fkey,
      DROP CONSTRAINT IF EXISTS user_controller_assignments_unit_id_fkey;
  `);
  console.log('✓ Temporarily dropped user_controller_assignments FK constraints');

  let inserted = 0;
  const progress = startProgress(rows.length, 'assignments  ');

  for (const row of rows) {
    // 不写 id —— 交给 SERIAL（见头部 O6）
    await pgClient.query(
      `INSERT INTO user_controller_assignments (
         user_id,
         unit_id,
         permission,
         send_sms_alarm,
         send_email_alarm
       )
       VALUES ($1,$2,$3,$4,$5)`,
      [
        row.UserId,
        row.UnitId,
        row.permission,
        row.sendSmsAlarm,
        row.sendEmailAlarm,
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);

  // ── 恢复外键前先诊断孤儿引用 ────────────────────────────────────────────────
  const orphanUser = await pgClient.query(`
    SELECT a.user_id, a.unit_id
      FROM user_controller_assignments a
     WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = a.user_id)
     ORDER BY a.user_id;
  `);
  if (orphanUser.rows.length > 0) {
    console.warn(`\n⚠ ${orphanUser.rows.length} 行的 user_id 在 users 里找不到`);
    console.warn('  （通常是该账号的 Email 为空、被 migration-user.js 丢弃了）');
    console.warn('  user_id\tunit_id');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanUser.rows) {
      console.warn(`  ${r.user_id}\t\t${r.unit_id}`);
    }
  }

  const orphanUnit = await pgClient.query(`
    SELECT a.user_id, a.unit_id
      FROM user_controller_assignments a
     WHERE NOT EXISTS (SELECT 1 FROM controllers c WHERE c.unit_id = a.unit_id)
     ORDER BY a.unit_id;
  `);
  if (orphanUnit.rows.length > 0) {
    console.warn(`\n⚠ ${orphanUnit.rows.length} 行的 unit_id 在 controllers 里找不到：`);
    console.warn('  user_id\tunit_id');
    console.warn('  ' + '-'.repeat(36));
    for (const r of orphanUnit.rows) {
      console.warn(`  ${r.user_id}\t\t${r.unit_id}`);
    }
  }

  if (orphanUser.rows.length > 0 || orphanUnit.rows.length > 0) {
    throw new Error(
      `Cannot restore FK constraints: found ${orphanUser.rows.length} orphaned user_id(s) ` +
      `and ${orphanUnit.rows.length} orphaned unit_id(s). See details above.`,
    );
  }

  // 恢复外键（ON DELETE CASCADE，与 initDatabase.js 一致）
  await pgClient.query(`
    ALTER TABLE user_controller_assignments
      ADD CONSTRAINT user_controller_assignments_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(user_id)           ON DELETE CASCADE,
      ADD CONSTRAINT user_controller_assignments_unit_id_fkey
        FOREIGN KEY (unit_id) REFERENCES controllers(unit_id)     ON DELETE CASCADE;
  `);
  console.log('✓ Restored user_controller_assignments FK constraints');
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateAssignments(pgClient);
    await pgClient.query('COMMIT');

    console.log('\n✓ migration-assignment completed.');
    console.log('Next: 把 "UserIdentity" 的 passkey / google 绑定搬到 user_identities');
    console.log('      （SQL Server 没有这份数据，任何迁移脚本都不会重建它）');
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
