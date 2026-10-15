/**
 * migration-user.js
 *
 * Migrate users: SQL Server (AquariusEmailDB) → PostgreSQL (AquariusPG)
 *
 * Source : dbo.UserInfomation  (WHERE Email IS NOT NULL)
 * Target : PostgreSQL public.users
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md
 * 目标 schema：src/initDatabase.js　**派生规则全部来自 src/derive.js**
 *
 * Column mapping:
 *   UserId                → user_id
 *   CompanyId             → company_id           原值（挂在 3001 也无所谓，见下）
 *   Email                 → email
 *   Activated=1           → email_verified
 *   Mobile                → mobile               空则 '0000000000'
 *   IsPhoneVerified       → mobile_verified
 *   Name→UserName→Email   → name                 显示名（derive.deriveDisplayName）
 *   UserName              → user_name            老站登录名
 *   PasswordHash          → password_hash
 *   Logo                  → **不迁**。老库 Logo 是公司 logo（→ companies.logo_url），avatar_url 是 Pro 独有、初始 NULL
 *   JWCode                → jw_code         🆕
 *   UserTypeId            → roles                见 derive.deriveRoles
 *   UserTypeId+CompanyId  → managed_company_ids  见 derive.deriveManagedCompanyIds
 *   Activated             → activated
 *   RequiresTwoFactorAuth → mfa_enabled
 *   LastLoginTime         → last_login_at        墙上时间换算成真时刻（derive.wallClockToInstant）
 *   DateCreated           → created_at
 *   LastUpdateDate        → updated_at           源列可空，空则回退 DateCreated

 *
 * ⚠️ 2026-09-09 按 David Grevell 的权限模型邮件重写，三处变化：
 *
 * ① **roles 改由 UserTypeId 派生，不再读 Production，也不再有 'owner'。**
 *    旧规则 `Production=1 → ['manager','owner']` 把 243 个 Owner 全变成 Manager，
 *    导致挂在同一 CompanyId 下的客户互相可见（3001 一家 71 个客户、1373 台设备）。
 *    Owner 是 per-controller 关系，运行时按 controllers.owner_user_id 判定。
 *
 * ② **加 jw_code。** 邮件："Jiwa Code should be linked to Owner -> so maybe in
 *    userinfomation. Companies don't need a jiwa code."
 *
 * ③ **company_id 保持源库原值。** 它的唯一用途是"Manager 能管哪些用户"
 *    （managedCompanyIds[] ↔ UserInfo.companyId）。个人客户挂在 3001 无害 ——
 *    只要 roles 不再把他们当 Manager。
 *
 * ⚠️ 重复邮箱**去重而不中止**（与 sync-simple.js 行为一致）。同步是无人值守跑的，
 *    中止会让整个同步停摆；两边必须用同一套去重逻辑，否则幂等性无从保证。
 *
 * Run:
 *   node src/migration-user.js        ← 必须先于 migration-company.js
 */

import dotenv from 'dotenv';
import { Pool } from 'pg';
import sql from 'mssql';
import { startProgress } from './progress.js';
import {
  deriveRoles,
  deriveManagedCompanyIds,
  deriveJwCode,
  deriveDisplayName,
  deriveLoginName,
  deriveMobile,
  dedupeUsersByEmail,
  toBool,
  wallClockToInstant,
} from './derive.js';
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

async function migrateUsers(pgClient) {
  console.log('Connecting to SQL Server…');
  const sqlPool = await sql.connect(sqlConfig);
  console.log(`✓ Connected: ${sqlConfig.server}/${sqlConfig.database}`);

  const result = await sqlPool.request().query(`
    SELECT
      UserId,
      CompanyId,
      UserTypeId,
      Email,
      Mobile,
      IsPhoneVerified,
      UserName,
      Name,
      PasswordHash,
      Logo,
      JWCode,
      Activated,
      RequiresTwoFactorAuth,
      LastLoginTime,
      DateCreated,
      COALESCE(LastUpdateDate, DateCreated) AS LastUpdateDate
    FROM ${SRC_DB}.dbo.UserInfomation
    WHERE Email IS NOT NULL
  `);

  const sourceRows = result.recordset;
  await sqlPool.close();

  const { rows, dropped } = dedupeUsersByEmail(sourceRows);
  console.log(`✓ Read ${sourceRows.length} source rows → ${rows.length} after email dedup`);

  if (dropped.length > 0) {
    console.warn(`\n⚠ ${dropped.length} 组重复邮箱（忽略大小写），已按"Activated > CorporateManager > 较大 UserId"取一：`);
    console.warn('  email                                    保留    丢弃');
    console.warn('  ' + '-'.repeat(60));
    for (const d of dropped.slice(0, 30)) {
      console.warn(`  ${d.email.padEnd(40)} ${String(d.keptUserId).padEnd(7)} ${d.droppedUserId}`);
    }
    if (dropped.length > 30) console.warn(`  …还有 ${dropped.length - 30} 组`);
    console.warn('  这些账号建议在 SQL Server 上合并或停用 —— 被丢弃的一方在 Pro 里无法登录。\n');
  }

  if (rows.length === 0) {
    console.log('⚠ No rows to migrate.');
    return;
  }

  let inserted = 0;
  const roleTally = new Map();
  const progress = startProgress(rows.length, 'users        ');

  for (const row of rows) {
    const roles = deriveRoles(row.UserTypeId);
    const managedCompanyIds = deriveManagedCompanyIds(row.UserTypeId, row.CompanyId);
    roleTally.set(roles.join('+'), (roleTally.get(roles.join('+')) ?? 0) + 1);

    await pgClient.query(
      `INSERT INTO users (
         user_id,
         company_id,
         email,
         email_verified,
         mobile,
         mobile_verified,
         name,
         user_name,
         password_hash,
         avatar_url,
         jw_code,
         roles,
         managed_company_ids,
         activated,
         mfa_enabled,
         last_login_at,
         created_at,
         updated_at
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
      [
        row.UserId,
        row.CompanyId,
        row.Email,
        toBool(row.Activated),                    // email_verified
        deriveMobile(row.Mobile),
        toBool(row.IsPhoneVerified),              // mobile_verified
        deriveDisplayName(row),                   // name（显示名）
        deriveLoginName(row),                     // user_name（老站登录名）
        row.PasswordHash || '',
        null,                                     // avatar_url（Pro 独有，Q18）
        deriveJwCode(row.JWCode),                 // jw_code
        roles,
        managedCompanyIds,
        toBool(row.Activated),
        toBool(row.RequiresTwoFactorAuth),        // mfa_enabled
        wallClockToInstant(row.LastLoginTime),    // last_login_at（墙上时间 → 真时刻）
        row.DateCreated,                          // created_at
        row.LastUpdateDate,                       // updated_at（SELECT 里已 COALESCE）
      ],
    );
    inserted++;
    progress.tick();
  }
  progress.done();

  console.log(`✓ Inserted : ${inserted}`);
  console.log('  角色分布：' + [...roleTally.entries()].map(([r, n]) => `${r}=${n}`).join('  '));
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const pgClient = await pgPool.connect();

  try {
    await pgClient.query('BEGIN');
    await migrateUsers(pgClient);
    await pgClient.query('COMMIT');

    console.log('\n✓ migration-user completed.');
    console.log('Next: node src/migration-company.js  （它会顺带恢复 companies 的外键）');
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
