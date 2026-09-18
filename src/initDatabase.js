/**
 * initDatabase.js
 *
 * 从零建库：删除并重建 AquariusPG 里 Aquareporter-pro 使用的全部表/视图/触发器。
 *
 * ⚠️ 会清空目标数据库里这几张表的数据。已有环境请用
 *    migrations/001_rename_to_snake_case.sql 做原地改名，不要跑这个脚本。
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md
 *   · 全部 lower_snake_case，永不加引号
 *   · 表名复数
 *   · 跨系统业务标识用 <entity>_id，纯内部代理键用 id
 *   · 约束/索引不显式命名，交给 PG 生成默认名
 *
 * 不由本脚本管理的表（adminpage 自有，手工建的）：
 *   admin_user_roles · admin_role_permissions · firmware_update_logs
 *
 * Run: npm run init-db
 */

import dotenv from 'dotenv';
import pg from 'pg';
import { getPostgresUrl } from './dbConfig.js';
import { COMM_TYPE, COMM_TYPE_VALUES, COMM_TYPE_EXCLUSIVE } from './derive.js';

dotenv.config();

const { Client } = pg;

async function initDatabase() {
  const client = new Client({
    connectionString: getPostgresUrl(),
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  console.log('Connected to PostgreSQL');

  try {
    await client.query('BEGIN');

    // ── 1. Drop views ────────────────────────────────────────────────────────
    console.log('Dropping views...');
    await client.query(`DROP VIEW IF EXISTS controllers_missing_subscription CASCADE;`);
    await client.query(`DROP VIEW IF EXISTS controller_subscription_status   CASCADE;`);
    await client.query(`DROP VIEW IF EXISTS active_user_controllers     CASCADE;`);
    await client.query(`DROP VIEW IF EXISTS controller_user_permissions CASCADE;`);

    // ── 2. Drop tables (children first) ──────────────────────────────────────
    console.log('Dropping tables...');
    await client.query(`DROP TABLE IF EXISTS user_identities             CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS user_mfa_factors            CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS user_controller_assignments CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS controllers                 CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS companies                   CASCADE;`);
    await client.query(`DROP TABLE IF EXISTS users                       CASCADE;`);

    await client.query(`DROP SEQUENCE IF EXISTS users_user_id_seq CASCADE;`);
    await client.query(`DROP FUNCTION IF EXISTS set_updated_at()   CASCADE;`);

    console.log('All objects dropped.');

    // ── 3. Shared trigger function ───────────────────────────────────────────
    // 所有表的时间戳列都叫 updated_at，一个函数就够了（改名前 controllers 用的是
    // DateLastUpdated，需要单独一个函数，现在不需要了）。
    // ⚠️ 这个触发器**只装在 Pro 自有的表上**（user_mfa_factors）。
    //    四张镜像表 —— users / companies / controllers / user_controller_assignments
    //    —— **刻意不装**，原因：
    //
    //    这些表的 updated_at 应当表示"**源记录**什么时候被改的"，值由 sqltopg 从
    //    SQL Server 的 LastUpdateDate / DateLastUpdate 显式带过来。自动打 NOW()
    //    会把它盖成"上次同步的时间"，这个列也就没有信息量了。
    //
    //    条件式触发器（"调用方没给值才打 NOW()"）也解决不了：只有 last_login_at
    //    变化的行，传入的 updated_at 与旧值相同，触发器仍会认为"调用方没改"而盖上
    //    NOW() —— 老站每次登录都会走到这条路径。
    //
    //    也不要用 `SET session_replication_role = replica` 来绕（那是复制作业的
    //    常规手法，AqpgUser 确实有权限）：它会连**外键检查一起关掉**，为一个时间戳
    //    语义丢掉四张表的外键保护不值得。
    //
    //    ⚠️ 代价：**应用层对这四张表的 UPDATE 必须自己写 `updated_at = NOW()`**。
    //       见台账 R-209。
    //
    // 函数本身写成条件式：调用方显式给了值就尊重它（对 user_mfa_factors 无影响，
    // 应用层从不传这个字段）。
    await client.query(`
      CREATE OR REPLACE FUNCTION set_updated_at()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
          NEW.updated_at = NOW();
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // ── 4. users ─────────────────────────────────────────────────────────────
    //
    // user_id 没有 DEFAULT，是刻意的：
    //   · 双跑期由 SQL Server 的 IDENTITY 分配，PG 侧必须显式给值 —— 万一
    //     代码漏传，这里会直接 NOT NULL 报错，而不是静默分配一个撞号的 id。
    //   · pg-only 模式下 PgWriteGateway 会显式调 nextval('users_user_id_seq')。
    //   · 切到 PG 权威（P6）后可以执行：
    //       ALTER TABLE users ALTER COLUMN user_id SET DEFAULT nextval('users_user_id_seq');
    //
    // ⚠️ 没有 sync_source 列。users 整表来自 SQL Server，不存在 Pro 自建的行，
    //    孤儿清理直接按"源库里还在不在"判断即可。
    //    （user_controller_assignments 保留了这一列 —— 那张表上 Pro 确实会写
    //     sync_source='pro' 的行，见该表的说明。）
    //
    // roles —— 只有 super / manager / operator 三个值，由 UserTypeId 派生（见
    //   src/derive.js 的 deriveRoles）。**没有 owner** —— Owner 是 per-controller
    //   的关系（controllers.owner_user_id），不是用户属性。
    //
    // managed_company_ids —— ⚠️ **Pro 独有数据，增量同步永不覆盖**。SQL Server
    //   里没有对应字段。初值由 derive.js 的 deriveManagedCompanyIds 给
    //   （CorporateManager → [本人公司]），之后只由 Pro 的界面维护 —— 邮件里
    //   Nick Duncan / Jay Dean 要管 HydroChem 多家分公司，只能人工配。
    //
    // jw_code —— Jiwa 往来户编码。邮件："Jiwa Code should be linked to Owner
    //   -> so maybe in userinfomation." 所以挂在 user 上，companies 表没有这一列。
    console.log('Creating users...');
    await client.query(`CREATE SEQUENCE users_user_id_seq AS integer;`);
    await client.query(`
      CREATE TABLE users (
        user_id             INTEGER      PRIMARY KEY,
        company_id          INTEGER      NOT NULL,
        email               VARCHAR(100) NOT NULL UNIQUE,
        email_verified      BOOLEAN      NOT NULL DEFAULT false,
        mobile              VARCHAR(50)  DEFAULT '0000000000',
        mobile_verified     BOOLEAN      NOT NULL DEFAULT false,
        name                VARCHAR(100) NOT NULL,
        password_hash       VARCHAR(255) NOT NULL,
        avatar_url          VARCHAR(255),
        jw_code             VARCHAR(50),
        roles               VARCHAR[]    NOT NULL DEFAULT ARRAY['operator']::varchar[],
        managed_company_ids INTEGER[],
        activated           BOOLEAN      NOT NULL DEFAULT true,
        mfa_enabled         BOOLEAN      NOT NULL DEFAULT false,
        linked_providers    VARCHAR[]    NOT NULL DEFAULT '{}',
        last_login_at       TIMESTAMPTZ,
        created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`ALTER SEQUENCE users_user_id_seq OWNED BY users.user_id;`);
    await client.query(`CREATE INDEX ON users (company_id);`);
    await client.query(`CREATE INDEX ON users (activated);`);
    // 代码里到处 email.toLowerCase()，但上面的 UNIQUE 是大小写敏感的
    await client.query(`CREATE UNIQUE INDEX users_email_lower_idx ON users (lower(email));`);
    // Manager 路径判定 (company_id = ANY(managed_company_ids)) 每次列表查询都要跑
    await client.query(`CREATE INDEX ON users USING GIN (managed_company_ids);`);
    await client.query(`CREATE INDEX ON users USING GIN (roles);`);

    // ── 5. companies ─────────────────────────────────────────────────────────
    //
    // 数据源是 dbo.CompanyInfomation（185 家，name 取真名 CompanyName）。
    //
    // ⚠️ 不要再从 `UserInfomation WHERE Production=1` 派生。那样只能得到 146 家，
    //    公司名取的是**用户名**会张冠李戴（CompanyId 2006 实为 Integra，却可能被
    //    命名成 "TerraCom Resources"），而且 39 家公司彻底丢失 —— 有 181 个用户和
    //    52 台设备引用着那 39 家。改用 CompanyInfomation 后引用零悬空。
    //
    // ⚠️ 没有 jw_code 列。邮件（David Grevell）："Companies don't need a jiwa
    //    code." JWCode 在 users.jw_code。
    //
    // ⚠️ name 上**刻意没有 UNIQUE**。源库确实存在重名（2026-09-09 核实：
    //    'Rockhampton Regional Council' 有 CompanyId 2036 和 2092 两条 —— 2013 年
    //    那条已停用，2017 年重建了一条）。公司的身份是 company_id，全部代码都按
    //    company_id 查询、name 只用于展示（已 grep 确认 Pro 与 adminpage 无一例外）。
    //    加 UNIQUE 只会让同步服务在下次有人建重名公司时硬失败。
    console.log('Creating companies...');
    await client.query(`
      CREATE TABLE companies (
        company_id      SERIAL       PRIMARY KEY,
        name            VARCHAR(200) NOT NULL,
        manager_user_id INTEGER      REFERENCES users(user_id) ON DELETE SET NULL,
        description     VARCHAR(200),
        phone           VARCHAR(50)  DEFAULT '0000000000',
        email           VARCHAR(100),
        logo_url        VARCHAR(500) DEFAULT 'YourLogoGoesToHere.JPG',
        activated       BOOLEAN      NOT NULL DEFAULT true,
        created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`CREATE INDEX ON companies (name);`);

    // ── 6. user_identities ───────────────────────────────────────────────────
    // 每个外部/绑定的登录方式一行：
    //   provider = 'password' | 'google' | 'passkey' | …
    //
    // Passkey (WebAuthn) 行：
    //   provider_user_id → base64url(credentialId)  （同 provider 内唯一）
    //   friendly_name    → 用户填的标签，如 "Hunter's MacBook"
    //   identity_data    → JSONB：
    //     { "publicKey": "<base64url COSE>", "counter": 0,
    //       "transports": ["internal","hybrid"],
    //       "deviceType": "singleDevice|multiDevice",
    //       "backedUp": true, "aaguid": "<uuid>" }
    console.log('Creating user_identities...');
    await client.query(`
      CREATE TABLE user_identities (
        id                      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id                 INTEGER      NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
        provider                VARCHAR(50)  NOT NULL,
        provider_user_id        VARCHAR(512) NOT NULL,
        provider_email          VARCHAR(100),
        provider_email_verified BOOLEAN,
        friendly_name           VARCHAR(120),
        identity_data           JSONB,
        linked_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        last_used_at            TIMESTAMPTZ,
        UNIQUE (provider, provider_user_id)
      );
    `);
    await client.query(`CREATE INDEX ON user_identities (user_id);`);
    await client.query(`CREATE INDEX ON user_identities (provider);`);
    await client.query(`CREATE INDEX ON user_identities (user_id, provider);`);

    // ── 7. user_mfa_factors ──────────────────────────────────────────────────
    console.log('Creating user_mfa_factors...');
    await client.query(`
      CREATE TABLE user_mfa_factors (
        id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id          INTEGER     NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
        factor_type      VARCHAR(50) NOT NULL,
        enabled          BOOLEAN     NOT NULL DEFAULT true,
        verified         BOOLEAN     NOT NULL DEFAULT false,
        is_primary       BOOLEAN     NOT NULL DEFAULT false,
        secret_encrypted TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`CREATE INDEX ON user_mfa_factors (user_id);`);
    await client.query(`CREATE INDEX ON user_mfa_factors (factor_type);`);
    await client.query(`
      CREATE TRIGGER user_mfa_factors_set_updated_at
        BEFORE UPDATE ON user_mfa_factors
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    `);

    // ── 8. controllers ───────────────────────────────────────────────────────
    //
    // 三处相对旧 schema 的语义修正：
    //   production_status ← 旧名 Status。存的是 SQL Server 的 ProductionStatus
    //                       (0=暂停 3=生产中 4=已完成)，与"设备通讯状态"无关。
    //   site_location     ← 旧名 Creator。存的是 SQL Server 的 SiteLocation，
    //                       叫 Creator 是同步脚本的失误。
    //   company_id        ← 取 **ControllerInfo.CompanyId 原值**（"这台设备由哪家
    //                       公司管理"），不是 Owner 所属公司。这是 Manager 权限的
    //                       判据（managed_company_ids ∋ company_id），取错会让
    //                       两站的可见范围不一致 —— 生产数据里两者有 223 台不同。
    //   system_type       ← 旧名 FirmwareVersion。**装的不是固件版本**，而是设备的
    //                       功能类型（CT / PRO / SP / ADV …共 13 个取值）。真正的固件
    //                       版本在 MongoDB 的 ControllerStatus.FirmwareVer（'4:26081916'
    //                       这种），firmware_version 这个名字留给它。
    //                       公司自己的 DDLSetupVersionTable.SystemType 用的就是同一套
    //                       词汇，沿用该叫法。
    //   model_code        ← 旧名 ControllerModel。生产物料型号（ULTIMA3P / BSMARTAS6
    //                       …共 175 种），编码了产品系列＋泵数＋通信＋变体。
    //                       清洗规则见 derive.js 的 deriveControllerModel。
    //   comm_types        ← 从 SIMCardNo **拆出来的新列**（数组）。老库把"通信方式"和
    //                       "SIM 卡号"挤在同一列（选 Ethernet 就往 SIMCardNo 里
    //                       写字符串 'Ethernet'），2026-10 的通信套餐要按
    //                       Ethernet / WiFi 收费，判据不能靠字符串嗅探。
    //                       拆分规则见 derive.js 的 deriveCommType。
    //                       用数组是因为一台设备可以有多种通信方式（以太网 + 备用
    //                       4G），生产数据里已有 4 台 'OwnSimandEth,'。计费判据
    //                       写成 comm_types && ARRAY['ethernet','wifi'] 即可。
    //
    // sim_card_no 因此**从 NOT NULL 改为可空** —— 只有真的填了卡号才有值，
    //   'Ethernet' / 'NOSIMCARD' / 'None' 这些非卡号的值不再往这一列灌。
    //   原始字符串不再镜像到 PG（曾有过一个 comm_raw 列，已去掉）；需要追溯
    //   'EthUpgApr19' 这类升级备注时回 SQL Server 的 dbo.ControllerInfo 查。
    //
    // owner_user_id ← AccManagerId。Owner 的权限是**运行时**按
    //   `owner_user_id == user_id` 判定的，不写进 users.roles（见 derive.js）。
    console.log('Creating controllers...');
    await client.query(`
      CREATE TABLE controllers (
        unit_id           INTEGER      PRIMARY KEY,
        serial_no         INTEGER,
        sim_card_no       VARCHAR(50),
        comm_types        VARCHAR(20)[] NOT NULL DEFAULT ARRAY['none']::varchar[]
                            CHECK (comm_types <@ ARRAY['wifi', 'ethernet', 'sim', 'modem_no_sim', 'none', 'other']::varchar[])
                            CHECK (array_length(comm_types, 1) >= 1)
                            CHECK (NOT (comm_types && ARRAY['none', 'other']::varchar[])
                                   OR array_length(comm_types, 1) = 1),
        owner_user_id     INTEGER      REFERENCES users(user_id)         ON DELETE SET NULL,
        company_id        INTEGER      REFERENCES companies(company_id)  ON DELETE SET NULL,
        system_id         VARCHAR(100) NOT NULL,
        system_type       VARCHAR(20),
        model_code        VARCHAR(50),
        timezone_id       VARCHAR(50),
        production_status INTEGER,
        activated         BOOLEAN      NOT NULL DEFAULT false,
        site_location     VARCHAR(255),
        tags              TEXT[],
        billing_email     VARCHAR(100),
        bar_code          VARCHAR(50),
        po_number         VARCHAR(50),
        spare_parts       VARCHAR(200),
        notes             TEXT,
        created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`CREATE INDEX ON controllers (owner_user_id);`);
    await client.query(`CREATE INDEX ON controllers (company_id);`);
    await client.query(`CREATE INDEX ON controllers (activated);`);
    // "哪些 Eth/WiFi 设备还没录通信套餐"这张报表按 comm_types 做集合相交
    // （comm_types && ARRAY['ethernet','wifi']），GIN 是这类查询的索引类型
    await client.query(`CREATE INDEX ON controllers USING GIN (comm_types);`);
    await client.query(`CREATE INDEX ON controllers (created_at);`);

    // ── 9. user_controller_assignments ───────────────────────────────────────
    //
    //   permission (SMALLINT) — 数值分级，越大权限越高。留空位便于以后插入新档
    //   （如 VIEWER=20 / EDITOR=70）而不用改 schema。
    //
    //       0  READONLY  只读
    //      50  NORMAL    默认工作权限
    //     100  OWNER     设备所有者
    //
    //   应用代码请引用 domain/permission.ts 里的常量，不要在调用点硬编码数字。
    //
    //   id 是纯 PG 序列（旧版迁移时直填了 SQL Server 的 SeqKey，双跑期会撞号）。
    //   跨库对齐用自然键 (user_id, unit_id)，也就是下面这个 UNIQUE。
    console.log('Creating user_controller_assignments...');
    await client.query(`
      CREATE TABLE user_controller_assignments (
        id               SERIAL      PRIMARY KEY,
        user_id          INTEGER     NOT NULL REFERENCES users(user_id)          ON DELETE CASCADE,
        unit_id          INTEGER     NOT NULL REFERENCES controllers(unit_id)    ON DELETE CASCADE,
        permission       SMALLINT    NOT NULL DEFAULT 50 CHECK (permission IN (0, 50, 100)),
        send_sms_alarm   BOOLEAN     NOT NULL DEFAULT false,
        send_email_alarm BOOLEAN     NOT NULL DEFAULT false,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        sync_source      VARCHAR(10) NOT NULL DEFAULT 'mssql'
                           CHECK (sync_source IN ('mssql', 'pro')),
        UNIQUE (user_id, unit_id)
      );
    `);
    await client.query(`CREATE INDEX ON user_controller_assignments (user_id);`);
    await client.query(`CREATE INDEX ON user_controller_assignments (unit_id);`);
    await client.query(`CREATE INDEX ON user_controller_assignments (permission);`);

    // ── 10. controller_subscriptions —— 通信套餐订阅 ─────────────────────────
    //
    // 背景：2026-10-01 起，新出厂的以太网 / WiFi ULTIMA 随机附带 5 年 AquaReporter
    // 通信套餐，到期后按 1 / 5 / 10 年续费。
    //
    // ⚠️ 与 sync_runs 同样处理：`CREATE TABLE IF NOT EXISTS`，且**刻意不在第 2 节的
    //    DROP 列表里**。这是 Pro 自有的商务数据，SQL Server 里没有对应表，
    //    重建镜像表绝不能把它清掉 —— 这里面是钱。
    //
    // ⚠️ unit_id **故意不加外键**。第 2 节的 `DROP TABLE controllers CASCADE` 会把
    //    引用 controllers 的外键约束一并删除（表和数据不丢，但引用完整性没了，
    //    而且不报错）。于是每跑一次 init-db 约束就悄悄消失一次。改用应用层校验 +
    //    下面 controllers_missing_subscription 视图做定期孤儿检查。
    //
    // ── 几个设计决定 ─────────────────────────────────────────────────────────
    //
    // 仅追加（append-only）—— 续费是插一行，不是改一行。老库 ControllerInfo 上的
    //   RenewalInv / RenewalDate 两个平列只能记住最后一次，这正是它撑不住新需求的
    //   原因（表达不了起算日、期限、赠送 vs 付费、续费历史）。
    //
    // ordered_on vs starts_on —— 生产部门的 Release 只是出厂测试环节，不代表设备
    //   正式售出，所以起算日不能自动取 Release 时间。ordered_on 由销售录入；
    //   starts_on 默认等于它但可改（设备在经销商仓库躺半年的情况）。
    //
    // voided_at 而非 DELETE —— 财务数据录错了作废留痕，不物理删除。
    //
    // comm_types_at_sale —— 售出时的通信方式快照。设备日后升级通信方式不应改写
    //   历史订阅的计费依据。
    console.log('Creating controller_subscriptions (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS controller_subscriptions (
        id                 BIGSERIAL     PRIMARY KEY,
        unit_id            INTEGER       NOT NULL,
        kind               VARCHAR(20)   NOT NULL
                             CHECK (kind IN ('included', 'renewal')),
        term_years         SMALLINT      NOT NULL CHECK (term_years IN (1, 5, 10)),
        ordered_on         DATE          NOT NULL,
        starts_on          DATE          NOT NULL,
        ends_on            DATE          NOT NULL,
        invoice_no         VARCHAR(50),
        po_number          VARCHAR(50),
        sold_to_company_id INTEGER,
        comm_types_at_sale VARCHAR(20)[] NOT NULL,
        note               TEXT,
        entered_by         INTEGER,
        entered_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        voided_at          TIMESTAMPTZ,
        voided_by          INTEGER,
        voided_reason      TEXT,
        CHECK (ends_on > starts_on),
        CHECK ((voided_at IS NULL) = (voided_reason IS NULL))
      );
    `);
    // 一台设备只能有一条**有效的**赠送套餐 —— 防销售重复录入
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS controller_subscriptions_one_included_idx
        ON controller_subscriptions (unit_id)
        WHERE kind = 'included' AND voided_at IS NULL;`);
    // 同一张发票不该在同一台设备上录两次
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS controller_subscriptions_invoice_idx
        ON controller_subscriptions (unit_id, invoice_no)
        WHERE invoice_no IS NOT NULL AND voided_at IS NULL;`);
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_subscriptions_unit_idx
        ON controller_subscriptions (unit_id, starts_on DESC);`);
    // "还有几天到期"报表按 ends_on 排序
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_subscriptions_expiry_idx
        ON controller_subscriptions (ends_on)
        WHERE voided_at IS NULL;`);

    // ── 11. Views ────────────────────────────────────────────────────────────
    // 旧的 "UserFullInfo" 是 users 的纯投影，没有抽象价值，不再重建。
    console.log('Creating views...');
    await client.query(`
      CREATE VIEW controller_user_permissions AS
      SELECT a.id,
             a.user_id,
             u.name           AS user_name,
             u.email          AS user_email,
             a.unit_id,
             c.system_id,
             c.sim_card_no,
             a.permission,
             a.send_sms_alarm,
             a.send_email_alarm,
             a.created_at,
             a.updated_at
        FROM user_controller_assignments a
        JOIN users       u ON u.user_id = a.user_id
        JOIN controllers c ON c.unit_id = a.unit_id;
    `);
    await client.query(`
      CREATE VIEW active_user_controllers AS
      SELECT u.user_id,
             u.name       AS user_name,
             u.email      AS user_email,
             c.unit_id,
             c.system_id,
             c.sim_card_no,
             c.activated  AS controller_activated,
             a.permission,
             a.created_at AS assigned_at
        FROM users u
        JOIN user_controller_assignments a ON a.user_id = u.user_id
        JOIN controllers                 c ON c.unit_id = a.unit_id
       WHERE u.activated = true
         AND c.activated = true;
    `);

    // 每台设备的当前订阅状态。取 ends_on 最大的那条有效记录 —— 续费是插新行，
    // "当前到期日"就是所有未作废记录里最晚的那个。
    await client.query(`
      CREATE VIEW controller_subscription_status AS
      SELECT c.unit_id,
             c.system_id,
             c.comm_types,
             c.activated,
             c.production_status,
             c.company_id,
             s.id           AS latest_subscription_id,
             s.kind         AS latest_kind,
             s.term_years   AS latest_term_years,
             s.starts_on,
             s.ends_on      AS expires_on,
             s.invoice_no,
             (s.id IS NULL)             AS is_missing,
             (s.ends_on < CURRENT_DATE) AS is_expired,
             (s.ends_on - CURRENT_DATE) AS days_remaining
        FROM controllers c
        LEFT JOIN LATERAL (
               SELECT * FROM controller_subscriptions x
                WHERE x.unit_id = c.unit_id AND x.voided_at IS NULL
                ORDER BY x.ends_on DESC
                LIMIT 1
             ) s ON true;
    `);

    // 缺套餐报表 —— 人工录入的字段如果没有完整性检查就一定会烂掉，这张视图就是
    // 那个检查。口径：在网的以太网 / WiFi 设备而没有任何有效订阅。
    //
    // comm_types 里含 'other' 的设备**也列出来**：那是老数据没分类出来、需要人工
    // 认领的，与产线明确选了 'none' 的处置相反（见 derive.js 的说明）。
    await client.query(`
      CREATE VIEW controllers_missing_subscription AS
      SELECT unit_id, system_id, comm_types, company_id, production_status
        FROM controller_subscription_status
       WHERE is_missing
         AND activated = true
         AND comm_types && ARRAY['ethernet', 'wifi', 'other']::varchar[]
       ORDER BY unit_id;
    `);

    // ── 12. sync_runs —— 同步运行日志 ────────────────────────────────────────
    //
    // ⚠️ 用 CREATE TABLE IF NOT EXISTS，且**刻意不在第 2 节的 DROP 列表里**：
    //    这张表记的是运维历史，不是镜像数据。重建镜像表不该把同步历史一起清掉，
    //    否则"上次成功同步是什么时候"这个问题在每次 init-db 之后就无从查证了。
    //
    // 写入方是 src/syncLog.js，**用独立连接、在同步事务之外提交** —— 否则同步
    // 失败回滚时会把自己那条失败记录一起回滚掉，等于失败从不留痕。
    //
    // 一次运行一行：开始时插入 status='running'，结束时更新为 success / failed。
    // 进程被杀会留下一条永久 'running' 的行 —— 那本身就是有用的信号
    // （可以据此发现卡死或崩溃的运行）。
    console.log('Creating sync_runs (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS sync_runs (
        id             BIGSERIAL   PRIMARY KEY,
        started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at    TIMESTAMPTZ,
        duration_ms    INTEGER,
        status         VARCHAR(10) NOT NULL DEFAULT 'running'
                         CHECK (status IN ('running', 'success', 'failed')),
        trigger_source VARCHAR(20) NOT NULL DEFAULT 'scheduled'
                         CHECK (trigger_source IN ('scheduled', 'startup', 'manual')),
        source_db      VARCHAR(64),
        rows_read      INTEGER,
        rows_written   INTEGER,
        rows_deleted   INTEGER,
        tables         JSONB,
        error_message  TEXT,
        host           VARCHAR(128)
      );
    `);
    // 最常见的查询是"最近几次跑得怎么样"
    await client.query(`CREATE INDEX IF NOT EXISTS sync_runs_started_at_idx ON sync_runs (started_at DESC);`);
    await client.query(`CREATE INDEX IF NOT EXISTS sync_runs_status_idx     ON sync_runs (status);`);

    await client.query(`COMMENT ON TABLE  sync_runs                IS '每次同步运行一行。由 src/syncLog.js 在同步事务之外写入；init-db 不清空本表'`);
    await client.query(`COMMENT ON COLUMN sync_runs.tables         IS '每张表的明细：{"users":{"read":5072,"written":0,"deleted":0}, …}'`);
    await client.query(`COMMENT ON COLUMN sync_runs.trigger_source IS 'scheduled=每日定时 / startup=服务启动时 / manual=手工 npm run sync'`);
    await client.query(`COMMENT ON COLUMN sync_runs.status         IS 'running 长期不变说明该次运行崩溃或被杀'`);

    await client.query('COMMIT');
    console.log('Database initialized successfully.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Initialization failed, rolled back:', err.message);
    process.exit(1);
  } finally {
    await client.end();
  }
}

initDatabase();
