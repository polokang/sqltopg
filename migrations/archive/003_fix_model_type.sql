-- =============================================================================
-- 003_fix_model_type.sql
--
-- 修 controllers.model_type 的两个数据问题。等价于 src/derive.js 的
-- deriveControllerModel()，两处规则必须一致。
--
--   ① 去掉 nchar(30) 的尾随填充
--   ② 字面量 'NONE' → NULL
--   ③ 统一成大写（同一型号存在 Ultima3p / Ultima3P / ULTIMA3P 三种写法）
--
-- 幂等：可重复执行（第二次跑影响 0 行）。
--
-- 执行：
--   psql "$POSTGRESQL_URL" -v ON_ERROR_STOP=1 -f migrations/003_fix_model_type.sql
--
-- ⚠️ 全新环境不需要本文件 —— `npm run sync` 写进去的值已经是处理过的。
--    本文件是给**已经同步过、库里存着填充值**的环境用的。
-- =============================================================================

BEGIN;

-- ── 1. 去掉尾随填充 ──────────────────────────────────────────────────────────
--
-- 源列 dbo.ControllerInfo.ControllerModel 是 nchar(30)，定长类型，mssql 驱动
-- 把值连同尾随空格一起返回（"ULTIMA3P" + 22 个空格）。旧版同步代码
-- （row.ControllerModel || null）原样写入，于是 PG 里每一行都是 30 字符。
--
-- 后果：WHERE model_type = 'ULTIMA3P' 匹配不到任何行，必须写 btrim(...)。
-- 2026-09-18 实测 aquariuspg_test 里 7980 行全部中招。

UPDATE controllers
   SET model_type = NULLIF(btrim(model_type), '')
 WHERE model_type IS NOT NULL
   AND model_type <> btrim(model_type);

-- ── 2. 字面量 'NONE' → NULL ──────────────────────────────────────────────────
--
-- 源库有 255 台的 ControllerModel 是字符串 'NONE' 而不是 NULL —— 那是"没填"的
-- 占位写法。照抄过来会让 PG 里多出一个叫 "NONE" 的合法型号，统计和下拉框都会
-- 把它当真型号。
--
-- 只认 'NONE'（忽略大小写）。'N/A' / '-' 这类占位符在生产数据里不存在
-- （2026-09-18 实查），不做推测性处理。

UPDATE controllers
   SET model_type = NULL
 WHERE model_type IS NOT NULL
   AND upper(btrim(model_type)) = 'NONE';

-- ── 3. 统一成大写 ───────────────────────────────────────────────────────────
--
-- 源库同一型号存在多种大小写写法（'Ultima3p' / 'Ultima3P' / 'ULTIMA3P'）。
-- SQL Server 默认排序规则 SQL_Latin1_General_CP1_CI_AS 大小写不敏感，把它们
-- 当成同一个值，所以旧站看不出问题；PG 区分大小写，同一型号会在统计里裂成
-- 2-3 行、下拉框出现重复项、按型号筛选漏数据。
--
-- 2026-09-18 实查：13 组型号有变体，共 2964 台设备。
--
-- ⚠️ 本步骤**不会**合并 'ULTIMA 3P'（带空格）与 'ULTIMA3P' —— 是不是同一型号
--    属于业务判断，涉及 2 台，留给人工决定。

UPDATE controllers
   SET model_type = upper(model_type)
 WHERE model_type IS NOT NULL
   AND model_type <> upper(model_type);

COMMIT;

-- ── 验证 ─────────────────────────────────────────────────────────────────────
-- SELECT count(*) FILTER (WHERE model_type <> btrim(model_type)) AS 仍带填充,
--        count(*) FILTER (WHERE upper(model_type) = 'NONE')      AS 仍为NONE,
--        count(*) FILTER (WHERE model_type <> upper(model_type)) AS 仍非大写,
--        count(*) FILTER (WHERE model_type IS NULL)              AS 为NULL,
--        count(DISTINCT model_type)                              AS 型号种类
--   FROM controllers;
-- 期望：0 / 0 / 0 / 315 / 176   （源库 60 个 NULL + 255 个 'NONE'）
