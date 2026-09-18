-- =============================================================================
-- 002_comm_types_and_subscriptions.sql
--
-- 给**已有**的 PG 环境原地升级，不重建表。等价于 initDatabase.js 的第 8/10/11 节。
--   · controllers 增加 comm_types（数组），sim_card_no 去掉 NOT NULL
--   · 从既有的 sim_card_no 回填这两列
--   · users / controllers 去掉 sync_source（user_controller_assignments 保留）
--   · 新建 controller_subscriptions 及其索引
--   · 新建 controller_subscription_status / controllers_missing_subscription 视图
--
-- 幂等：可重复执行。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/002_comm_types_and_subscriptions.sql
--   （连接串里若带 ?schema=public 要去掉，换成 ?sslmode=require）
--
-- ⚠️ 全新环境请直接跑 `npm run init-db`，不要用本文件。
-- =============================================================================

BEGIN;

-- ── 1. controllers.comm_types ────────────────────────────────────────────────
--
-- 老库把"通信方式"和"SIM 卡号"挤在 SIMCardNo 一列里（adminpage 选 Ethernet 就把
-- 字符串 'Ethernet' 写进去）。这里拆开。
--
-- 用数组而非单值：一台设备可以同时具备多种通信方式。生产数据实测有 6 台
-- （'OwnSimandEth,' 4 台、'NOSIMCARD,WIFI' 1 台、'NowWifiPrevModem' 1 台）。
-- 计费判据写成 comm_types && ARRAY['ethernet','wifi'] 即可。

ALTER TABLE controllers ALTER COLUMN sim_card_no DROP NOT NULL;

ALTER TABLE controllers
  ADD COLUMN IF NOT EXISTS comm_types VARCHAR(20)[] NOT NULL DEFAULT ARRAY['none']::varchar[];

-- 早期版本有过一个 comm_raw 列（原样保留 SIMCardNo 字符串），已废弃。
-- 需要追溯 'EthUpgApr19' 这类升级备注时回 SQL Server 的 dbo.ControllerInfo 查。
ALTER TABLE controllers DROP COLUMN IF EXISTS comm_raw;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'controllers_comm_types_valid') THEN
    ALTER TABLE controllers ADD CONSTRAINT controllers_comm_types_valid
      CHECK (comm_types <@ ARRAY['wifi','ethernet','sim','modem_no_sim','none','other']::varchar[]);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'controllers_comm_types_nonempty') THEN
    ALTER TABLE controllers ADD CONSTRAINT controllers_comm_types_nonempty
      CHECK (array_length(comm_types, 1) >= 1);
  END IF;
  -- 'none' / 'other' 是对整台设备的判断，不能和确定的通信方式并列
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'controllers_comm_types_exclusive') THEN
    ALTER TABLE controllers ADD CONSTRAINT controllers_comm_types_exclusive
      CHECK (NOT (comm_types && ARRAY['none','other']::varchar[])
             OR array_length(comm_types, 1) = 1);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS controllers_comm_types_idx ON controllers USING GIN (comm_types);

-- ── 2. 回填 ──────────────────────────────────────────────────────────────────
--
-- 规则必须与 src/derive.js 的 deriveCommType 逐条一致，否则回填结果和下一轮同步
-- 结果会打架，数据每 2 分钟翻来翻去。改一处就要改两处。
--
--   ① 数字开头                → {sim}，原值留在 sim_card_no
--   ② 包含 'wifi'             → 追加 wifi
--   ③ 包含 'eth'              → 追加 ethernet
--   ④ 包含 'sim' 或 'modem'   → 追加 modem_no_sim
--   ⑤ 都没命中：空 / 'none' → {none}，其余 → {other}
--
-- ⚠️ ① 必须先判。实查 1899 个 distinct 值是数字开头且含字母（卡号带后缀与联系
--    电话），共 1994 台；先跑关键字匹配会把它们判错。

WITH n AS (
  SELECT unit_id,
         BTRIM(COALESCE(sim_card_no, ''))                                         AS raw,
         LOWER(REGEXP_REPLACE(COALESCE(sim_card_no,''), '[^A-Za-z0-9]', '', 'g')) AS norm
    FROM controllers
), t AS (
  SELECT unit_id, raw, norm,
         CASE WHEN norm LIKE '%wifi%' THEN ARRAY['wifi'] ELSE ARRAY[]::text[] END
       || CASE WHEN norm LIKE '%eth%'  THEN ARRAY['ethernet'] ELSE ARRAY[]::text[] END
       || CASE WHEN norm LIKE '%sim%' OR norm LIKE '%modem%'
               THEN ARRAY['modem_no_sim'] ELSE ARRAY[]::text[] END AS kw
    FROM n
)
UPDATE controllers c
   SET comm_types = CASE
         WHEN t.raw ~ '^[0-9]'                      THEN ARRAY['sim']::varchar[]
         WHEN array_length(t.kw, 1) >= 1            THEN t.kw::varchar[]
         WHEN t.norm = '' OR t.norm = 'none'        THEN ARRAY['none']::varchar[]
         ELSE                                            ARRAY['other']::varchar[]
       END,
       -- 只有真的填了卡号才留在 sim_card_no，'Ethernet' 这类值清空
       sim_card_no = CASE WHEN t.raw ~ '^[0-9]' THEN t.raw ELSE NULL END
  FROM t
 WHERE c.unit_id = t.unit_id
   -- ⚠️ 幂等护栏：只处理**尚未拆分**的行。
   --    拆分后 sim_card_no 对非卡号的值已置 NULL，再拿它重算会得到 {none} ——
   --    没有这个条件，本脚本重跑一次就会把 wifi / ethernet / modem_no_sim
   --    全部打回 {none}（2026-09-16 实测会误伤 2139 行）。
   AND c.comm_types = ARRAY['none']::varchar[]
   AND t.raw <> '';

-- ── 3. 去掉 users / controllers 的 sync_source ───────────────────────────────
--
-- 这两张表整表来自 SQL Server，不存在 Pro 自建的行，孤儿清理直接按"源库里还在
-- 不在"判断即可。
--
-- ⚠️ user_controller_assignments **保留**这一列：Pro 确实会往那张表写
--    sync_source='pro' 的行（Manager 的"偏好-only"授权），无条件清理会静默删掉。

ALTER TABLE users       DROP CONSTRAINT IF EXISTS users_sync_source_check;
ALTER TABLE controllers DROP CONSTRAINT IF EXISTS controllers_sync_source_check;
ALTER TABLE users       DROP COLUMN IF EXISTS sync_source;
ALTER TABLE controllers DROP COLUMN IF EXISTS sync_source;

-- ── 4. controller_subscriptions ──────────────────────────────────────────────
--
-- 2026-10-01 起新出厂的以太网 / WiFi ULTIMA 随机附带 5 年通信套餐，到期后按
-- 1 / 5 / 10 年续费。
--
-- ⚠️ unit_id **故意不加外键**：initDatabase.js 的 `DROP TABLE controllers CASCADE`
--    会把引用 controllers 的外键约束静默删除（不报错），于是约束每次 init-db
--    就消失一次。改用应用层校验 + controllers_missing_subscription 视图做孤儿检查。
--
-- 仅追加：续费是插一行而不是改一行。老库 ControllerInfo 的 RenewalInv /
-- RenewalDate 两个平列只记得住最后一次，表达不了起算日、期限与续费历史。

CREATE TABLE IF NOT EXISTS controller_subscriptions (
  id                 BIGSERIAL     PRIMARY KEY,
  unit_id            INTEGER       NOT NULL,
  kind               VARCHAR(20)   NOT NULL CHECK (kind IN ('included', 'renewal')),
  term_years         SMALLINT      NOT NULL CHECK (term_years IN (1, 5, 10)),
  -- ordered_on 由销售录入。生产部门的 Release 只是出厂测试环节，不代表设备正式
  -- 售出，所以起算日不能自动取 Release 时间。
  ordered_on         DATE          NOT NULL,
  -- 默认等于 ordered_on，但可改 —— 设备可能在经销商仓库躺半年才到客户手上。
  starts_on          DATE          NOT NULL,
  ends_on            DATE          NOT NULL,
  invoice_no         VARCHAR(50),
  po_number          VARCHAR(50),
  sold_to_company_id INTEGER,
  -- 售出时的通信方式快照：设备日后升级通信方式，不应改写历史订阅的计费依据。
  comm_types_at_sale VARCHAR(20)[] NOT NULL,
  note               TEXT,
  entered_by         INTEGER,
  entered_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  -- 财务数据录错了作废留痕，不物理删除。
  voided_at          TIMESTAMPTZ,
  voided_by          INTEGER,
  voided_reason      TEXT,
  CHECK (ends_on > starts_on),
  CHECK ((voided_at IS NULL) = (voided_reason IS NULL))
);

-- 一台设备只能有一条有效的赠送套餐 —— 防销售重复录入
CREATE UNIQUE INDEX IF NOT EXISTS controller_subscriptions_one_included_idx
  ON controller_subscriptions (unit_id)
  WHERE kind = 'included' AND voided_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS controller_subscriptions_invoice_idx
  ON controller_subscriptions (unit_id, invoice_no)
  WHERE invoice_no IS NOT NULL AND voided_at IS NULL;

CREATE INDEX IF NOT EXISTS controller_subscriptions_unit_idx
  ON controller_subscriptions (unit_id, starts_on DESC);

CREATE INDEX IF NOT EXISTS controller_subscriptions_expiry_idx
  ON controller_subscriptions (ends_on)
  WHERE voided_at IS NULL;

-- ── 5. 视图 ──────────────────────────────────────────────────────────────────

-- 当前订阅状态：取 ends_on 最大的那条有效记录。续费是插新行，所以"当前到期日"
-- 就是所有未作废记录里最晚的那个。
DROP VIEW IF EXISTS controllers_missing_subscription;
DROP VIEW IF EXISTS controller_subscription_status;

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

-- 缺套餐报表。人工录入的字段没有完整性检查就一定会烂掉，这张视图就是那个检查。
-- 含 'other' 的设备也列出来：那是老数据没分类出来、需要人工认领的，与产线明确
-- 选了 'none' 的处置相反。
CREATE VIEW controllers_missing_subscription AS
SELECT unit_id, system_id, comm_types, company_id, production_status
  FROM controller_subscription_status
 WHERE is_missing
   AND activated = true
   AND comm_types && ARRAY['ethernet', 'wifi', 'other']::varchar[]
 ORDER BY unit_id;

COMMIT;

-- ── 验证 ─────────────────────────────────────────────────────────────────────
-- SELECT comm_types, COUNT(*) FROM controllers GROUP BY 1 ORDER BY 2 DESC;
-- SELECT COUNT(*) FROM controllers_missing_subscription;
