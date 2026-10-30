/**
 * initDatabase.js
 *
 * 从零建库：删除并重建 AquariusPG 里 Aquareporter-pro 使用的全部表/视图/触发器。
 *
 * ⚠️ 会清空下面"重建的表"的数据（跑完要 `npm run sync` 重新灌）。不能清空的环境
 *    用 migrations/ 下的幂等脚本做原地升级（目前是 004、005）。
 *
 * 重建的表（DROP + CREATE）：users · companies · controllers · user_controller_assignments ·
 *   user_identities · user_mfa_factors，以及 4 个视图。
 *   ⚠️ users.managed_company_ids 是 Pro 人工维护的（不变式 I3），重建后回到派生初值，
 *      有人工配置时先备份。
 * 跨重建保留的表（CREATE TABLE IF NOT EXISTS）：controller_subscriptions ·
 *   controller_service_logs · controller_service_tasks · admin_controller_notes · sync_runs
 *
 * 命名规范：Aquareporter-pro/docs/migration/05-pg-naming-convention.md
 *   · 全部 lower_snake_case，永不加引号
 *   · 表名复数
 *   · 跨系统业务标识用 <entity>_id，纯内部代理键用 id
 *   · 约束/索引不显式命名，交给 PG 生成默认名
 *
 * 不由本脚本管理、也不会被它碰到的表（adminpage 自有，见 adminpage-api/prisma/pg/*.sql）：
 *   admin_roles · admin_permissions · admin_role_permissions · admin_user_roles ·
 *   firmware_update_logs · product_labels · product_serial_counters · admin_operation_logs
 *
 * Run: npm run init-db
 */

import dotenv from 'dotenv';
import pg from 'pg';
import { getPostgresUrl } from './dbConfig.js';
import {
  COMM_TYPE, COMM_TYPE_VALUES, COMM_TYPE_EXCLUSIVE,
  CASH_ACCOUNT_USER_ID,
} from './derive.js';

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
    // sync_source —— 2026-10-14 重新加回（方案 A 阶段 0.2，修同步竞态）。
    //    sqltopg 每轮"先读快照 → UPSERT → 孤儿清理"。Pro 双写在"读快照"之后
    //    新建的用户不在快照里，旧的无条件清理会把它当孤儿删掉（连带 CASCADE 掉授权）。
    //    现在：Pro 写入标 'pro'；孤儿清理只删 'mssql' 行；下一轮同步 UPSERT 命中
    //    时改回 'mssql'（见 upserts.js），之后源库删了它才会正常清理。
    //    controllers / user_controller_assignments 同理（不变式 I2，见 sync-simple.js）。
    //    ⚠️ 只存在于 PG、长期停在 'pro' 的行 = Pro 双写漏了权威库，由对账发现。
    //
    // name / user_name —— 2026-10-15 拆开（project-docs 06 W2）：
    //   name      显示名  ← UserInfomation.Name（回退 UserName → Email）
    //   user_name 登录名  ← UserInfomation.UserName（老站按 CompanyId + UserName 登录，
    //                       只在公司内唯一，所以这里不加 UNIQUE）
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
        user_name           VARCHAR(50),
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
        updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        sync_source         VARCHAR(10)  NOT NULL DEFAULT 'mssql'
                              CHECK (sync_source IN ('mssql', 'pro'))
      );
    `);
    await client.query(`ALTER SEQUENCE users_user_id_seq OWNED BY users.user_id;`);
    await client.query(`CREATE INDEX ON users (company_id);`);
    await client.query(`CREATE INDEX ON users (activated);`);
    // Pro 登录支持按老站登录名（UserName）查找
    await client.query(`CREATE INDEX ON users (user_name);`);
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
    //   creator           ← SiteLocation。⚠️ 这一列**不是站点位置**，装的是建档人：
    //                       实查只有 5 个取值 Other(6681) / Vish(724) / Andrew(515) /
    //                       Brad(48) / Sooni(30)，与 adminpage 的 operatorOptions 完全一致
    //                       （adminpage-web 的 Edit.tsx，界面上标的是 "operator"）。
    //                       旧同步脚本曾叫它 Creator，后来被"更正"成 site_location ——
    //                       那次更正是错的，2026-09-18 已改回。
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
    //   accessory_serial_numbers ← 旧名 spare_parts（源列 Serials）。装的是随机附带
    //                       的部件序列号清单，不是"备件库存"，mapping.json 2026-09-21
    //                       改成了现在这个名字。切分规则见 derive.js 的 classifySerials。
    //   billing_email     ← Owner（AccManagerId）在 UserInfomation 里的 Email，派生值，
    //                       ControllerInfo 没有这一列（R-004）。Pro 只写 PG 的修改会被同步盖回。
    //   notes             ← **这一列没有了**。ControllerInfo.Notes 一格里塞着一台设备
    //                       的全部历史（段间用 '-->' 分隔），已按段拆进
    //                       admin_controller_notes（见第 13 节）。
    //
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
        creator           VARCHAR(50),
        tags              TEXT[],
        billing_email     VARCHAR(100),
        bar_code          VARCHAR(50),
        po_number         VARCHAR(50),
        accessory_serial_numbers TEXT[],
        created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        sync_source       VARCHAR(10)  NOT NULL DEFAULT 'mssql'
                            CHECK (sync_source IN ('mssql', 'pro'))
      );
    `);
    await client.query(`CREATE INDEX ON controllers (owner_user_id);`);
    await client.query(`CREATE INDEX ON controllers (company_id);`);
    await client.query(`CREATE INDEX ON controllers (activated);`);
    // "哪些 Eth/WiFi 设备还没录通信套餐"这张报表按 comm_types 做集合相交
    // （comm_types && ARRAY['ethernet','wifi']），GIN 是这类查询的索引类型
    await client.query(`CREATE INDEX ON controllers USING GIN (comm_types);`);
    // "哪台设备装了序列号 X" —— accessory_serial_numbers 是数组，用 GIN
    await client.query(`CREATE INDEX ON controllers USING GIN (accessory_serial_numbers);`);
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
    //
    // paid_by_user_id —— **这笔钱是谁付的**。这张表现在只记设备的订阅，以后要
    //   接用户付费记录 —— 付费人这一维度必须在设计阶段就在，事后补就只能
    //   把已有的几百条历史全算到一个默认人头上。
    //
    //   迁过来的 legacy 行取设备的 owner（ControllerInfo.AccManagerId）—— 2026-09-22
    //   实测 379 台有 Renewal 记录的设备**全部**有 owner，一条都不用兜底。
    //   没有付费用户时记在公司自己的现金账户名下（derive.js 的
    //   CASH_ACCOUNT_USER_ID = 5040，'Spare Parts' / 'Aquarius Corp Acc'）。
    //   留 NULL 是最差的选择：“这笔钱没人付”和“不知道谁付的”分不开。
    //
    // ⚠️ 不加外键，理由同 unit_id。
    console.log('Creating controller_subscriptions (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS controller_subscriptions (
        id                 BIGSERIAL     PRIMARY KEY,
        unit_id            INTEGER       NOT NULL,
        -- 'legacy' 专给从 ControllerInfo.RenewalInv / RenewalDate 迁过来的历史 SIM 续费。
        -- 源库只有"发票号 + 到期日"两列，没有期限也没有起算日 —— 缺的字段一律留空，
        -- 不编一个出来。新录入的必须是 included / renewal 且字段齐全（见下方 CHECK）。
        kind               VARCHAR(20)   NOT NULL
                             CONSTRAINT subs_kind_valid
                             CHECK (kind IN ('included', 'renewal', 'legacy')),
        term_years         SMALLINT      CONSTRAINT subs_term_valid
                             CHECK (term_years IN (1, 5, 10)),
        ordered_on         DATE,
        starts_on          DATE,
        -- 到期日是任何来源都有的字段，所以只有它保持 NOT NULL
        ends_on            DATE          NOT NULL,
        invoice_no         VARCHAR(50),
        po_number          VARCHAR(50),
        sold_to_company_id INTEGER,
        comm_types_at_sale VARCHAR(20)[] NOT NULL,
        note               TEXT,
        paid_by_user_id    INTEGER       NOT NULL DEFAULT ${CASH_ACCOUNT_USER_ID},
        entered_by         INTEGER,
        entered_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        voided_at          TIMESTAMPTZ,
        voided_by          INTEGER,
        voided_reason      TEXT,
        CONSTRAINT subs_dates_ordered CHECK (starts_on IS NULL OR ends_on > starts_on),
        CONSTRAINT subs_void_paired   CHECK ((voided_at IS NULL) = (voided_reason IS NULL)),
        -- 只有 legacy 允许缺字段；新录入的必须齐全，否则到期提醒和续费金额都算不出来
        CONSTRAINT subs_complete_unless_legacy
          CHECK (kind = 'legacy'
                 OR (term_years IS NOT NULL AND ordered_on IS NOT NULL AND starts_on IS NOT NULL))
      );
    `);
    // ⚠️ 上面是 CREATE TABLE IF NOT EXISTS —— **表已存在时整段会被跳过**，
    //    改了列定义也不会生效。所以凡是"跨重建保留"的表，都要在这里补一段幂等的
    //    ALTER 把已有表升级到当前定义。2026-09-18 就是漏了这一步，导致放宽约束后
    //    迁移仍然报 term_years not-null。
    //
    //    新增列用 ADD COLUMN IF NOT EXISTS；改约束则先按名字 DROP 再 ADD，
    //    所以上面的 CHECK 都起了显式名字。
    console.log('  ↻ 升级 controller_subscriptions 到当前定义…');
    await client.query(`
      ALTER TABLE controller_subscriptions
        ALTER COLUMN term_years DROP NOT NULL,
        ALTER COLUMN ordered_on DROP NOT NULL,
        ALTER COLUMN starts_on  DROP NOT NULL,
        ADD COLUMN IF NOT EXISTS paid_by_user_id INTEGER NOT NULL
          DEFAULT ${CASH_ACCOUNT_USER_ID};`);
    // 老版本的匿名 CHECK（PG 自动命名 *_check）要一并清掉，否则旧规则还在生效
    await client.query(`
      DO $$
      DECLARE c record;
      BEGIN
        FOR c IN
          SELECT conname FROM pg_constraint
           WHERE conrelid = 'controller_subscriptions'::regclass
             AND contype = 'c'
             AND conname NOT LIKE 'subs\\_%'
        LOOP
          EXECUTE format('ALTER TABLE controller_subscriptions DROP CONSTRAINT %I', c.conname);
        END LOOP;
      END $$;`);
    for (const [name, expr] of [
      ['subs_kind_valid', `kind IN ('included','renewal','legacy')`],
      ['subs_term_valid', `term_years IN (1,5,10)`],
      ['subs_dates_ordered', `starts_on IS NULL OR ends_on > starts_on`],
      ['subs_void_paired', `(voided_at IS NULL) = (voided_reason IS NULL)`],
      ['subs_complete_unless_legacy',
        `kind = 'legacy' OR (term_years IS NOT NULL AND ordered_on IS NOT NULL AND starts_on IS NOT NULL)`],
    ]) {
      await client.query(`
        DO $$ BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_constraint
                          WHERE conrelid = 'controller_subscriptions'::regclass
                            AND conname = '${name}') THEN
            ALTER TABLE controller_subscriptions ADD CONSTRAINT ${name} CHECK (${expr});
          END IF;
        END $$;`);
    }

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
    // “这个人（或现金账户）名下有哪些订阅”—— 接用户付费记录后的主要查法
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_subscriptions_payer_idx
        ON controller_subscriptions (paid_by_user_id, ends_on DESC)
        WHERE voided_at IS NULL;`);
    // "还有几天到期"报表按 ends_on 排序
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_subscriptions_expiry_idx
        ON controller_subscriptions (ends_on)
        WHERE voided_at IS NULL;`);

    // ── 11. controller_service_logs —— 设备服务记录（面向客户）─────────────────
    //
    // 数据源：ControllerNotes 整表（682 行 / 313 台）。内容是服务工程师写的现场记录
    // （"setpoint raised to 695mV"、"CS41 replaced; calibrated pH"），面向**客户**。
    //
    // ⚠️ 与 sync_runs / controller_subscriptions 同样处理：CREATE TABLE IF NOT EXISTS，
    //    且**不在第 2 节的 DROP 列表里**。一次性迁移之后由应用继续写入，
    //    重建镜像表不能把它清掉。
    //
    // ⚠️ unit_id / created_by **不加外键** —— 第 2 节的 DROP … CASCADE 会把引用
    //    controllers / users 的外键约束静默删除（表和数据不丢，但约束没了且不报错）。
    //
    // source + source_id 让一次性迁移可以重复执行：同一条 ControllerNotes.ID
    // 再迁一次会撞唯一索引而被跳过，不会产生重复行。
    console.log('Creating controller_service_logs (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS controller_service_logs (
        id                  BIGSERIAL   PRIMARY KEY,
        unit_id             INTEGER     NOT NULL,
        body                TEXT        NOT NULL,
        occurred_on         DATE,
        created_by          INTEGER,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        -- 个别记录是内部升级讨论，不该给客户看；默认可见、个别置否
        visible_to_customer BOOLEAN     NOT NULL DEFAULT true,
        source              VARCHAR(30) NOT NULL DEFAULT 'manual'
                              CHECK (source IN ('manual', 'controller_notes')),
        source_id           INTEGER
      );
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS controller_service_logs_source_idx
        ON controller_service_logs (source, source_id)
        WHERE source_id IS NOT NULL;`);
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_service_logs_unit_idx
        ON controller_service_logs (unit_id, created_at DESC);`);

    // ── 12. controller_service_tasks —— 派工任务（面向客户）────────────────────
    //
    // 数据源：ControllerInfo.Action（263 台）。内容是技术支持记录的处理动作
    // （"Tech Advised - No Flow 25/6/26"）。
    //
    // ⚠️ 旧实现 ControllerService.ActionSave 用的是 `.Set(unit => unit.Action, action)`
    //    —— **覆盖写**，每记一次新动作上一次就被抹掉。所以迁过来的 263 条只是"最后
    //    一次"，更早的历史在源库里就已经没了。新表按行追加，不再有这个问题。
    //
    // status 里的 'unknown' 是给迁移数据用的：那 263 条没有任何完成信息，
    // 标 'done' 是编造、标 'open' 会凭空造出 263 个未完成任务，都不诚实。
    // 它的行数只应随人工认领而下降，可以当清理进度看。
    console.log('Creating controller_service_tasks (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS controller_service_tasks (
        id                  BIGSERIAL   PRIMARY KEY,
        unit_id             INTEGER     NOT NULL,
        body                TEXT        NOT NULL,
        status              VARCHAR(20) NOT NULL DEFAULT 'open'
                              CHECK (status IN ('open','in_progress','done','cancelled','unknown')),
        assigned_to         INTEGER,
        due_on              DATE,
        occurred_on         DATE,
        created_by          INTEGER,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at        TIMESTAMPTZ,
        completed_by        INTEGER,
        completion_note     TEXT,
        visible_to_customer BOOLEAN     NOT NULL DEFAULT true,
        source              VARCHAR(30) NOT NULL DEFAULT 'manual'
                              CHECK (source IN ('manual', 'controller_info_action')),
        source_id           INTEGER,
        -- 标成已完成就必须有完成时间；其余状态不强制
        CHECK (status <> 'done' OR completed_at IS NOT NULL)
      );
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS controller_service_tasks_source_idx
        ON controller_service_tasks (source, source_id)
        WHERE source_id IS NOT NULL;`);
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_service_tasks_unit_idx
        ON controller_service_tasks (unit_id, created_at DESC);`);
    await client.query(`
      CREATE INDEX IF NOT EXISTS controller_service_tasks_open_idx
        ON controller_service_tasks (status, assigned_to)
        WHERE status IN ('open','in_progress');`);

    // ── 13. admin_controller_notes —— 生产/内部备注（仅内部可见）──────────────
    //
    // 数据源：ControllerInfo.Notes（7409 台有值）。⚠️ 与 controller_service_logs
    // 的区别只有受众：那张表是服务工程师写给**客户**看的现场记录，这张表是产线和
    // 后台人员写给**自己**看的，绝不能出现在客户界面上。所以两者不合并。
    //
    // ⚠️ 源列是一格里塞了一台设备的全部历史，段与段之间用 '-->' 分隔，最长一台
    //    有 39 段。照抄成一个 TEXT 列（原来 controllers.notes 就是这么干的）的
    //    后果是：查不了"某台设备 2026-02 发生过什么"、排不了序、也没法单独标记
    //    某一段。这里按段拆成行，拆分与过滤规则见 derive.js 的 parseControllerNotes。
    //
    // ⚠️ 与 sync_runs / controller_subscriptions / controller_service_* 同样处理：
    //    CREATE TABLE IF NOT EXISTS，且**不在第 2 节的 DROP 列表里**。
    //
    // ⚠️ unit_id / created_by **不加外键** —— 第 2 节的 DROP … CASCADE 会把引用
    //    controllers / users 的外键约束静默删掉（表和数据不丢，但约束没了且不报错）。
    //
    // 自然键是 (source, source_id, source_seq)：source_id = UnitId，source_seq 是
    // **过滤后**的段序号。新备注一律追加在末尾，已有段的序号不会移动，所以同步
    // 可以按这个键幂等 UPSERT；源里段数变少时同步会删掉尾部多出来的行。
    // 应用自己写的行 source='manual' 且 source_id 为 NULL，落不进那个偏唯一索引，
    // **永远不会被同步动到**。
    //
    // ── created_by：写备注的人 ─────────────────────────────────────────────
    //
    // 源列没有作者。三条路都查过：`UserIdForLastChange` 只有 62/8045 台有值；
    // 正文结尾的首字母签名只覆盖 2.7% 且一半是 SN/ORP/PRO 这种技术缩写；
    // 能用的只有 OperationLog —— adminpage 每次改备注都会存下改前改后全文，
    // 把新出现的那一段认出来就知道是谁加的（derive.js 的 deriveNoteAuthors）。
    //
    // ⚠️ 只能覆盖 **16.8%**（1132/6744 段）—— OperationLog 最早到 2024-07，
    //    而备注能追到 2015。剩下的留 NULL，**不拿设备 owner 顶上** —— 备注是
    //    产线和后台人员写的，owner 是客户侧的客户经理，填了就是错的。
    //
    // ── occurred_on：这段备注讲的是哪一天 ───────────────────────────────
    //
    // 三级回退：① 能对上作者的用日志时间（最准）→ ② 从正文里抠出的澳式
    // 日期（'25/6/26'，见 parseNoteDate）→ ③ 设备的建档日期 DateCreated。
    //
    // ③ 是个占位值，不是真实发生日期 —— 判别方法是 `occurred_on = created_at::date`
    // （created_at 存的就是 DateCreated）。需要“只看有真实日期的”时把这批排掉即可。
    console.log('Creating admin_controller_notes (preserved across rebuilds)...');
    await client.query(`
      CREATE TABLE IF NOT EXISTS admin_controller_notes (
        id          BIGSERIAL   PRIMARY KEY,
        unit_id     INTEGER     NOT NULL,
        body        TEXT        NOT NULL,
        occurred_on DATE,
        created_by  INTEGER,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source      VARCHAR(30) NOT NULL DEFAULT 'manual'
                      CHECK (source IN ('manual', 'controller_info_notes')),
        source_id   INTEGER,
        source_seq  SMALLINT
      );
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS admin_controller_notes_source_idx
        ON admin_controller_notes (source, source_id, source_seq)
        WHERE source_id IS NOT NULL;`);
    await client.query(`
      CREATE INDEX IF NOT EXISTS admin_controller_notes_unit_idx
        ON admin_controller_notes (unit_id, source_seq);`);
    await client.query(`COMMENT ON TABLE  admin_controller_notes            IS '内部备注（旧 ControllerInfo.Notes 按 --> 拆行）。⚠️ 仅内部可见，不要暴露给客户界面'`);
    await client.query(`COMMENT ON COLUMN admin_controller_notes.source_seq  IS '该设备第几段备注（过滤后的稠密序号，从 0 起）'`);
    await client.query(`COMMENT ON COLUMN admin_controller_notes.created_by  IS '写备注的人。迁移行只有 16.8% 能从 OperationLog 反推出来，其余为 NULL'`);
    await client.query(`COMMENT ON COLUMN admin_controller_notes.occurred_on IS '日志时间 → 正文里的日期 → 设备建档日期。等于 created_at::date 时是兜底值，不是真实发生日'`);

    // ── 14. admin_operation_logs —— 已移交 adminpage ─────────────────────────
    //
    // 2026-10-30 起这张表归 adminpage 所有、由 adminpage 直接写入，结构也换了
    // （一次修改一行 + old_values / new_values），建表脚本在
    // adminpage-api/prisma/pg/006_admin_operation_logs.sql。本脚本不再建它、
    // 不再给它补约束，sync 也不再从 OperationLog 同步。
    // ⚠️ 不要把这里的旧建表语句加回来：IF NOT EXISTS 虽然会跳过建表，但后面
    //    按名字补 CHECK 约束那段会引用已经不存在的 source 列，init-db 会直接失败。


    // ── 15. Views ────────────────────────────────────────────────────────────
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

    // ── 16. sync_runs —— 同步运行日志 ────────────────────────────────────────
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
    await client.query(`COMMENT ON COLUMN sync_runs.trigger_source IS 'scheduled=按调度触发（间隔或每日定时）/ startup=服务启动时 / manual=手工 npm run sync'`);
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
