/**
 * upserts.js —— 四张镜像表的**批量** UPSERT 语句。
 *
 * ── 为什么要有这个文件 ───────────────────────────────────────────────────────
 *
 * 原来的写法是 `for (const row of rows) await pgClient.query(INSERT ...)`，
 * 一行一次往返。2026-09-09 实测（本机 → Azure PostgreSQL `aquariuspgdb`）：
 *
 *     PG 单次往返                       16.87 ms
 *     逐行 UPSERT 2000 行               34.5 秒   → 折算 3.2 万行 ≈ 551 秒（9 分钟）
 *     一条 jsonb_to_recordset 写 2000 行  28 ms   → 折算 3.2 万行 ≈ 0.44 秒
 *                                                  快 1253 倍
 *
 * 而源库全量读四张表只要 1.5 秒 —— 瓶颈完全在往返次数上，不在数据量。
 * 9 分钟的单轮耗时塞不进原来的 2 分钟调度周期（index.js 的 isRunning 保护会让它
 * 连续跳过周期，实际间隔变成 10 分钟左右）。
 *
 * ⚠️ 本地 Docker 容器的往返是 0.1 ms 级别，3.2 万次只要十几秒 —— **排练测不出
 *    这个问题**。这类问题只有对着真实延迟的库才会暴露。
 *
 * ── 为什么用 jsonb_to_recordset 而不是 unnest ────────────────────────────────
 *
 * `unnest($1::int[], $2::text[], …)` 需要每列一个数组，但 users.roles /
 * users.managed_company_ids / controllers.tags 本身就是**数组列** —— unnest 会把
 * 它们拍平，表达不了"每行一个数组"。JSON 天然支持嵌套，而且只需要一个参数。
 *
 * 数组列在 SQL 里用 `ARRAY(SELECT jsonb_array_elements_text(x.col))` 还原。
 * ⚠️ 空数组 `[]` 会得到 `{}`（空数组）而不是 NULL —— 必须如此，因为逐行写法
 *    产出的就是 `{}`（生产数据里 4905 行 managed_company_ids 和 5613 行 tags
 *    都是 `{}`）。两者不一致会让每轮同步都误判为"有变化"。
 *
 * ── DO UPDATE 的 WHERE 子句 ─────────────────────────────────────────────────
 *
 * 每条语句都带 `WHERE (…) IS DISTINCT FROM (…)`：**没有变化的行完全不写**。
 * 否则每轮把 3.2 万行重写一遍，WAL 与表膨胀之外，`rows_written` 这个观测指标
 * 也就没有意义了。
 *
 * ⚠️ users 的 DO UPDATE **刻意不含 managed_company_ids**（不变式 I3，见
 *    sync-simple.js 头部）：SQL Server 没有这个字段，它由 Pro 人工维护。
 *    它出现在 INSERT 的列里（新用户要有初值），但更新时绝不覆盖。
 */

/** 一次 statement 塞多少行。1000 行的 JSON 约 300 KB，足够小、又把往返压到个位数。 */
export const CHUNK_SIZE = 1000;

/**
 * 分批执行一条批量 UPSERT，返回**实际写入**的行数（未变化的行不计入）。
 *
 * @param {import('pg').PoolClient} pgClient  必须是持有事务的那个 client
 * @param {string} sql   本模块导出的语句之一，只接受一个 $1 = JSON 数组
 * @param {object[]} rows
 */
export async function upsertChunked(pgClient, sql, rows, chunkSize = CHUNK_SIZE) {
    let written = 0;
    for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        const res = await pgClient.query(sql, [JSON.stringify(chunk)]);
        written += res.rowCount;
    }
    return written;
}

// ─── companies ────────────────────────────────────────────────────────────────

export const UPSERT_COMPANIES = `
INSERT INTO companies
  (company_id, name, phone, email, logo_url, activated, created_at, updated_at)
SELECT x.company_id, x.name, x.phone, x.email, x.logo_url,
       x.activated, x.created_at, x.updated_at
  FROM jsonb_to_recordset($1::jsonb) AS x(
    company_id int, name varchar, phone varchar, email varchar,
    logo_url varchar, activated boolean,
    created_at timestamptz, updated_at timestamptz
  )
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
       EXCLUDED.logo_url, EXCLUDED.activated, EXCLUDED.updated_at)
`;

// ─── users ────────────────────────────────────────────────────────────────────

export const UPSERT_USERS = `
INSERT INTO users (
  user_id, company_id, email, email_verified, mobile, mobile_verified,
  name, password_hash, avatar_url, jw_code,
  roles, managed_company_ids, activated, mfa_enabled,
  last_login_at, created_at, updated_at
)
SELECT
  x.user_id, x.company_id, x.email, x.email_verified, x.mobile, x.mobile_verified,
  x.name, x.password_hash, x.avatar_url, x.jw_code,
  ARRAY(SELECT jsonb_array_elements_text(x.roles))::varchar[],
  ARRAY(SELECT jsonb_array_elements_text(x.managed_company_ids)::int),
  x.activated, x.mfa_enabled,
  x.last_login_at, x.created_at, x.updated_at
  FROM jsonb_to_recordset($1::jsonb) AS x(
    user_id int, company_id int, email varchar, email_verified boolean,
    mobile varchar, mobile_verified boolean, name varchar, password_hash varchar,
    avatar_url varchar, jw_code varchar,
    roles jsonb, managed_company_ids jsonb,
    activated boolean, mfa_enabled boolean,
    last_login_at timestamptz, created_at timestamptz, updated_at timestamptz
  )
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
       EXCLUDED.last_login_at, EXCLUDED.updated_at)
`;

// ─── controllers ──────────────────────────────────────────────────────────────

export const UPSERT_CONTROLLERS = `
INSERT INTO controllers (
  unit_id, serial_no, sim_card_no, comm_types, owner_user_id, company_id,
  system_id, system_type, model_code, timezone_id, production_status,
  activated, created_at, updated_at, site_location, tags, billing_email,
  bar_code, po_number, spare_parts, notes
)
SELECT
  x.unit_id, x.serial_no, x.sim_card_no,
  ARRAY(SELECT jsonb_array_elements_text(x.comm_types))::varchar[],
  x.owner_user_id, x.company_id,
  x.system_id, x.system_type, x.model_code, x.timezone_id, x.production_status,
  x.activated, x.created_at, x.updated_at, x.site_location,
  ARRAY(SELECT jsonb_array_elements_text(x.tags))::text[],
  x.billing_email,
  x.bar_code, x.po_number, x.spare_parts, x.notes
  FROM jsonb_to_recordset($1::jsonb) AS x(
    unit_id int, serial_no int, sim_card_no varchar, comm_types jsonb,
    owner_user_id int, company_id int,
    system_id varchar, system_type varchar, model_code varchar,
    timezone_id varchar, production_status int, activated boolean,
    created_at timestamptz, updated_at timestamptz, site_location varchar,
    tags jsonb, billing_email varchar,
    bar_code varchar, po_number varchar, spare_parts varchar, notes text
  )
ON CONFLICT (unit_id) DO UPDATE SET
  serial_no         = EXCLUDED.serial_no,
  sim_card_no       = EXCLUDED.sim_card_no,
  comm_types        = EXCLUDED.comm_types,
  owner_user_id     = EXCLUDED.owner_user_id,
  company_id        = EXCLUDED.company_id,
  system_id         = EXCLUDED.system_id,
  system_type       = EXCLUDED.system_type,
  model_code        = EXCLUDED.model_code,
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
WHERE (controllers.serial_no, controllers.sim_card_no,
       controllers.comm_types, controllers.owner_user_id,
       controllers.company_id, controllers.system_id,
       controllers.system_type, controllers.model_code, controllers.timezone_id,
       controllers.production_status,
       controllers.activated, controllers.updated_at, controllers.site_location,
       controllers.tags, controllers.billing_email, controllers.bar_code,
       controllers.po_number, controllers.spare_parts, controllers.notes)
  IS DISTINCT FROM
      (EXCLUDED.serial_no, EXCLUDED.sim_card_no,
       EXCLUDED.comm_types, EXCLUDED.owner_user_id,
       EXCLUDED.company_id, EXCLUDED.system_id,
       EXCLUDED.system_type, EXCLUDED.model_code, EXCLUDED.timezone_id,
       EXCLUDED.production_status,
       EXCLUDED.activated, EXCLUDED.updated_at, EXCLUDED.site_location,
       EXCLUDED.tags, EXCLUDED.billing_email, EXCLUDED.bar_code,
       EXCLUDED.po_number, EXCLUDED.spare_parts, EXCLUDED.notes)
`;

// ─── user_controller_assignments ──────────────────────────────────────────────

export const UPSERT_ASSIGNMENTS = `
INSERT INTO user_controller_assignments
  (user_id, unit_id, permission, send_sms_alarm, send_email_alarm)
SELECT x.user_id, x.unit_id, x.permission, x.send_sms_alarm, x.send_email_alarm
  FROM jsonb_to_recordset($1::jsonb) AS x(
    user_id int, unit_id int, permission smallint,
    send_sms_alarm boolean, send_email_alarm boolean
  )
ON CONFLICT (user_id, unit_id) DO UPDATE SET
  permission       = EXCLUDED.permission,
  send_sms_alarm   = EXCLUDED.send_sms_alarm,
  send_email_alarm = EXCLUDED.send_email_alarm
WHERE (user_controller_assignments.permission,
       user_controller_assignments.send_sms_alarm,
       user_controller_assignments.send_email_alarm)
  IS DISTINCT FROM
      (EXCLUDED.permission, EXCLUDED.send_sms_alarm, EXCLUDED.send_email_alarm)
`;
