-- =============================================================================
-- 004_admin_notes_and_operation_logs.sql
--
-- 落实 mapping.json 2026-09-21/22 的决策：
--
--   ① ControllerInfo.SiteLocation → controllers.creator
--      （旧名 site_location。这一列实测只有 5 个取值：Other / Vish / Andrew /
--       Brad / Sooni —— 是建档人，不是站点位置）
--   ② ControllerInfo.Serials → controllers.accessory_serial_numbers
--      （原来叫 spare_parts —— 装的是随机附带的部件序列号清单，不是备件库存）
--   ③ ControllerInfo.Notes → 新表 admin_controller_notes（按 '-->' 拆成多行）
--      controllers.notes 这一列就此取消
--   ④ OperationLog → 新表 admin_operation_logs（**摊平成字段级改动**，不是镜像）
--   ⑤ controller_subscriptions 新增 paid_by_user_id（这笔钱是谁付的）
--
-- ⚠️ **多数情况下你不需要这个文件。** 本项目的 schema 真相源是
--    `src/initDatabase.js`，变更一律走 `npm run init-db` 重建 —— 那个脚本已经
--    包含上面全部改动（三张保留表用 CREATE TABLE IF NOT EXISTS，不会被重建清掉）。
--
--    这个文件是给**不能重建镜像表的环境**用的：Aquareporter-pro 已经在写
--    user_controller_assignments 的 sync_source='pro' 行、或者 adminpage 已经
--    在用这几张表，`init-db` 的 DROP TABLE controllers CASCADE 代价太大时，
--    用它做原地升级。
--
-- 幂等：可重复执行（第二次跑什么都不做）。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/004_admin_notes_and_operation_logs.sql
--
-- 跑完必须再跑一次同步把两张新表填上：
--   npm run sync
--
-- ⚠️ 本脚本**不搬 controllers.notes 里的数据**，直接把那一列删掉。
--    拆分与过滤规则（逐键存盘噪音、字段变更回显…）只写在 src/derive.js 的
--    parseControllerNotes 里 —— 在 SQL 里再实现一遍就会有两份会漂的规则，
--    那正是 derive.js 这个文件存在的原因（不变式 I1）。源数据在 SQL Server 里
--    一个字都没少，`npm run sync` 会按同一份规则重新拆出来。
-- =============================================================================

BEGIN;

-- ── ① controllers.site_location → creator ───────────────────────────────────
--
-- 源列 ControllerInfo.SiteLocation 装的不是站点位置。实测（2026-09-22）只有五个
-- 取值：Other(6677) / Vish(737) / Andrew(514) / Brad(48) / Sooni(30)，与 adminpage
-- 界面上的 operatorOptions 一模一样 —— 它是建档人。叫 site_location 会让人拿它
-- 去做地域统计，结果是 6677 台设备全在一个叫 "Other" 的地方。

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = 'controllers'
                AND column_name = 'site_location')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = 'controllers'
                        AND column_name = 'creator') THEN
    ALTER TABLE controllers RENAME COLUMN site_location TO creator;
  END IF;
END $$;

-- ── ② controllers.spare_parts → accessory_serial_numbers ────────────────────
--
-- 源列 ControllerInfo.Serials 装的是"这台设备随机发出去的部件序列号"
-- （面板 / 调制解调器 / 传感器各一个），不是仓库备件。spare_parts 这个名字
-- 会让人以为可以拿它查库存。

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = 'controllers'
                AND column_name = 'spare_parts')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                      WHERE table_schema = 'public' AND table_name = 'controllers'
                        AND column_name = 'accessory_serial_numbers') THEN
    ALTER TABLE controllers RENAME COLUMN spare_parts TO accessory_serial_numbers;
  END IF;
END $$;

-- 索引跟着改名，纯粹为了以后 \d controllers 读得懂（不改也不影响查询）
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'controllers_spare_parts_idx')
     AND NOT EXISTS (SELECT 1 FROM pg_class
                      WHERE relname = 'controllers_accessory_serial_numbers_idx') THEN
    ALTER INDEX controllers_spare_parts_idx
      RENAME TO controllers_accessory_serial_numbers_idx;
  END IF;
END $$;

-- ── ③ admin_controller_notes —— 生产/内部备注 ───────────────────────────────
--
-- ⚠️ 与 controller_service_logs 的区别只有受众：那张表是服务工程师写给**客户**
--    看的现场记录，这张是产线和后台人员写给**自己**看的，绝不能出现在客户界面。
--
-- ⚠️ unit_id / created_by **不加外键**：`init-db` 的 DROP TABLE controllers
--    CASCADE 会把引用 controllers 的外键静默删掉（表和数据不丢，但约束没了、
--    还不报错），于是每跑一次约束就悄悄消失一次。改用应用层校验。
--
-- 自然键 (source, source_id, source_seq) = ('controller_info_notes', UnitId, 段序号)。
-- 段序号是过滤后的稠密序号；新备注一律追加在末尾，已有段的序号不会移动。
-- 应用自己写的行 source='manual' 且 source_id 为 NULL，落不进那个偏唯一索引，
-- 永远不会被同步动到。
--
-- created_by（写备注的人）：源列没有作者。能用的只有 OperationLog —— adminpage
-- 每次改备注都会存下改前改后全文，把新出现的那一段认出来就知道是谁加的。
-- ⚠️ 只能覆盖 16.8%（1132/6744 段），因为日志最早到 2024-07 而备注追到 2015。
--    其余留 NULL，**不拿设备 owner 顶上** —— 备注是产线人员写的，owner 是客户侧
--    的客户经理，填了就是错的。
--
-- occurred_on 三级回退：日志时间（最准）→ 正文里抠出的澳式日期 → 设备建档日期。
-- 最后一级是占位值，判别方法是 occurred_on = created_at::date。

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

CREATE UNIQUE INDEX IF NOT EXISTS admin_controller_notes_source_idx
  ON admin_controller_notes (source, source_id, source_seq)
  WHERE source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS admin_controller_notes_unit_idx
  ON admin_controller_notes (unit_id, source_seq);

COMMENT ON TABLE  admin_controller_notes             IS '内部备注（旧 ControllerInfo.Notes 按 --> 拆行）。⚠️ 仅内部可见，不要暴露给客户界面';
COMMENT ON COLUMN admin_controller_notes.source_seq  IS '该设备第几段备注（过滤后的稠密序号，从 0 起）';
COMMENT ON COLUMN admin_controller_notes.created_by  IS '写备注的人。迁移行只有 16.8% 能从 OperationLog 反推出来，其余为 NULL';
COMMENT ON COLUMN admin_controller_notes.occurred_on IS '日志时间 → 正文里的日期 → 设备建档日期。等于 created_at::date 时是兜底值，不是真实发生日';

-- controllers.notes 就此取消。数据由 `npm run sync` 从 SQL Server 重新拆出来，
-- 见文件头的说明。
ALTER TABLE controllers DROP COLUMN IF EXISTS notes;

-- ── ④ admin_operation_logs —— 后台字段级改动审计 ────────────────────────────
--
-- 一行回答一个问题：**谁 · 什么时候 · 把哪条记录的哪个字段 · 改成了什么**。
--
-- ⚠️ 这不是 SQL Server OperationLog 的镜像。那张表一行存两份整行快照（47-52 列
--    的 JSON），臃肿且查不动 —— 想知道"这台设备的 SystemID 被谁改过"只能
--    `WHERE updated_record::text LIKE '%…%'` 再人工比对两份 JSON。这里把它摊平成
--    字段级改动：一条源日志改了 3 个字段就是 3 行。
--
-- ⚠️ **不存改动前的值。** 需求只要"改成了什么"；存旧值要么整行翻倍，要么就得
--    保留那两份快照，正好是这次要甩掉的东西。
--
-- 迁移口径（规则在 src/derive.js 的 deriveOperationLogRows，只此一份）：
--   · 只迁设备与用户的修改，外加续费（UPDATE_RENEWAL 一条改多台，按台拆行）
--   · LOGIN / LOGINTOREPORT 不迁 —— 那两类的 UpdatedRecord 是 8 小时有效的登录
--     JWT 原文，每天同步等于把还能用的 bearer token 复制进一张随便谁都能查的表
--   · ADD_CONTROLLER / ADD_USER / RELEASE_CONTROLLER 不迁 —— 整条新增没有"改成
--     什么"可记，全展开是 6 万行建档噪音
--   · ADD_ACCESSORY / UPDATE_ACCESSORY 不迁 —— 不属于设备和用户的修改
--   · 明文 Password 列不记（mapping.json 已定为死列），只留 password_hash 一行、
--     值抹成 '***'
--
-- field 用的是 **PG 的列名**（`creator` 而不是 `SiteLocation`）。PG 里没有的列
-- 保持源列名（如 SetupVersionId）—— 那是当年真的改过的东西，不能因为现在不迁
-- 就假装没发生。

CREATE TABLE IF NOT EXISTS admin_operation_logs (
  id             BIGSERIAL   PRIMARY KEY,
  occurred_at    TIMESTAMPTZ NOT NULL,
  actor_user_id  INTEGER,
  operation_type VARCHAR(30) NOT NULL,
  target_type    VARCHAR(20) NOT NULL
                   CONSTRAINT admin_operation_logs_target_type_valid
                   CHECK (target_type IN ('controller', 'user')),
  target_id      INTEGER     NOT NULL,
  field          VARCHAR(50) NOT NULL,
  new_value      TEXT,
  source         VARCHAR(30) NOT NULL DEFAULT 'manual'
                   CONSTRAINT admin_operation_logs_source_valid
                   CHECK (source IN ('manual', 'operation_log')),
  source_id      INTEGER,
  source_seq     SMALLINT
);

CREATE UNIQUE INDEX IF NOT EXISTS admin_operation_logs_source_idx
  ON admin_operation_logs (source, source_id, source_seq)
  WHERE source_id IS NOT NULL;

-- "这台设备/这个用户被谁改过" —— 这张表存在的理由，也是唯一必须快的查询
CREATE INDEX IF NOT EXISTS admin_operation_logs_target_idx
  ON admin_operation_logs (target_type, target_id, occurred_at DESC);
-- "这个字段历史上都被改成过什么"（例如某台设备的 sim_card_no 变更史）
CREATE INDEX IF NOT EXISTS admin_operation_logs_field_idx
  ON admin_operation_logs (target_type, target_id, field, occurred_at DESC);
CREATE INDEX IF NOT EXISTS admin_operation_logs_actor_idx
  ON admin_operation_logs (actor_user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS admin_operation_logs_occurred_idx
  ON admin_operation_logs (occurred_at DESC);

COMMENT ON TABLE  admin_operation_logs               IS '后台字段级改动审计（旧 OperationLog 摊平）。⚠️ 仅内部可见；不存旧值；登录事件与新增事件不迁；明文密码抹成 ***';
COMMENT ON COLUMN admin_operation_logs.actor_user_id IS '做这次改动的人（源列 UserId），不是被改的对象';
COMMENT ON COLUMN admin_operation_logs.field         IS 'PG 列名；PG 里没有的列保持源列名（如 SetupVersionId）';
COMMENT ON COLUMN admin_operation_logs.new_value     IS '改成了什么。NULL = 被清空；密码类一律 ***';

-- ── ⑤ controller_subscriptions.paid_by_user_id —— 这笔钱是谁付的 ────────────
--
-- 这张表现在只记设备的订阅，以后要接用户付费记录 —— 付费人这一维度必须在设计
-- 阶段就在，事后补就只能把已有的几百条历史全算到一个默认人头上。
--
-- 迁过来的 legacy 行取设备的 owner（ControllerInfo.AccManagerId）：2026-09-22
-- 实测 379 台有 Renewal 记录的设备**全部**有 owner，一条都用不上兜底。没有付费
-- 用户时记在公司自己的现金账户名下（UserId 5040 = 'Spare Parts' /
-- 'Aquarius Corp Acc'）。留 NULL 是最差的选择：那样"这笔钱没人付"和"不知道谁
-- 付的"就分不开了。
--
-- ⚠️ 不加外键，理由同 unit_id（DROP … CASCADE 会静默删掉约束）。

ALTER TABLE controller_subscriptions
  ADD COLUMN IF NOT EXISTS paid_by_user_id INTEGER NOT NULL DEFAULT 5040;

-- 已有的 legacy 行补上真实付费人（新装的列此前一律是兜底的 5040）
UPDATE controller_subscriptions s
   SET paid_by_user_id = c.owner_user_id
  FROM controllers c
 WHERE c.unit_id = s.unit_id
   AND c.owner_user_id IS NOT NULL
   AND s.paid_by_user_id = 5040;

-- "这个人（或现金账户）名下有哪些订阅" —— 接用户付费记录后的主要查法
CREATE INDEX IF NOT EXISTS controller_subscriptions_payer_idx
  ON controller_subscriptions (paid_by_user_id, ends_on DESC)
  WHERE voided_at IS NULL;

COMMENT ON COLUMN controller_subscriptions.paid_by_user_id IS '付费人。迁移行取设备 owner；没有付费用户时记公司现金账户 UserId 5040';

COMMIT;

-- ── 验证 ─────────────────────────────────────────────────────────────────────
-- 先跑一次同步：npm run sync
--
-- SELECT count(*) AS 备注行数, count(DISTINCT unit_id) AS 台数,
--        count(created_by) AS 有作者, count(occurred_on) AS 有日期
--   FROM admin_controller_notes WHERE source = 'controller_info_notes';
-- 期望（2026-09-22 源库口径）：6744 / 6269 / 1132 / 6744
--
-- SELECT operation_type, count(*) FROM admin_operation_logs GROUP BY 1 ORDER BY 2 DESC;
-- 期望：合计 ≈ 9653 行（源表每天在长），且只有 MODIFY_CONTROLLER /
--       MODIFY_USER / ACTIVE_ / DEACTIVE_CONTROLLER / RESETPASSWORD_USER /
--       UPDATE_RENEWAL 六种
--
-- SELECT count(*) FROM admin_operation_logs
--  WHERE field ILIKE '%password%' AND new_value <> '***';
-- 期望：0
--
-- SELECT count(*) FROM controller_subscriptions WHERE paid_by_user_id IS NULL;
-- 期望：0（列是 NOT NULL）
--
-- SELECT count(*) FROM information_schema.columns
--  WHERE table_name = 'controllers'
--    AND column_name IN ('spare_parts', 'notes', 'site_location');
-- 期望：0
