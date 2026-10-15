/**
 * derive.js —— SQL Server → PostgreSQL 的**全部派生规则**，唯一一份。
 *
 * 为什么要有这个文件：
 *   一次性迁移（migration-*.js）和 2 分钟增量同步（sync-simple.js）必须产出
 *   **完全一致**的结果。规则写两遍就一定会漂，漂了的后果是数据在每轮同步里翻来
 *   翻去，而且极难排查。历史上这两边的邮箱去重逻辑就已经不一致了：
 *   sync 按优先级保留一行，migration 直接中止。
 *
 *   所以：任何"从源库字段算出目标库字段"的逻辑，只允许写在这里。
 *
 * 规则依据：
 *   · Aquareporter-pro/docs/PERMISSION_MODEL.md
 *   · David Grevell 2026-09 的权限模型邮件
 *   · Aquareporter-pro/docs/migration/05-pg-naming-convention.md
 */

// ─── UserTypes 字典（2026-09-09 从生产库 dbo.UserTypes 实读，不是猜的）───────
//
//   22 AquariusSuperUser        18 人
//   23 AquariusSalesSupport      1 人
//   24 CorporateManager        171 人  ← Manager 的唯一来源
//   25 AccountManager            0 人  ← 字典里有，无人使用
//   26 ServiceRepresentative  4846 人
//   27 CustomerContact          36 人
//
// ⚠️ docs/migration/01 里 25/26/27 的标签是从代码反推的，反推错了，以本表为准。

export const USER_TYPE = {
    AQUARIUS_SUPER_USER: 22,
    AQUARIUS_SALES_SUPPORT: 23,
    CORPORATE_MANAGER: 24,
    ACCOUNT_MANAGER: 25,
    SERVICE_REPRESENTATIVE: 26,
    CUSTOMER_CONTACT: 27,
};

/** PG 侧的权限分级，与 aquareporter-api/src/domain/permission.ts 保持一致。 */
export const PERMISSION = { READONLY: 0, NORMAL: 50, OWNER: 100 };

// ─── 通用小工具 ──────────────────────────────────────────────────────────────

/** SQL Server 的 BIT 可能以 1 / true / '1' / 'true' 回来。 */
export const toBool = (v) => v === 1 || v === true || v === '1' || v === 'true';

/** 空字符串和纯空格都算"没填"，统一成 null。 */
export const nullIfBlank = (v) =>
    v === null || v === undefined || String(v).trim() === '' ? null : String(v).trim();

// ─── 角色派生 ────────────────────────────────────────────────────────────────

/**
 * users.roles —— 只有三个值，全小写。
 *
 *   UserTypeId = 22 → ['super']
 *   UserTypeId = 24 → ['manager']
 *   其余            → ['operator']
 *
 * ⚠️ 两条与旧规则的重大差异：
 *
 * ① **不再有 'owner' 角色。** Owner 是 per-controller 的关系
 *    （`controllers.owner_user_id == user_id`），不是用户的属性 —— 同一个人
 *    可能是设备 1-3 的 Owner 而不是设备 4 的 Owner，放进 roles[] 表达不了。
 *    权限判定按 PERMISSION_MODEL.md 的四路径并集在运行时算。
 *
 * ② **不再读 Production。** 旧规则 `Production=1 → ['manager','owner']` 把 243 个
 *    Owner 全变成了 Manager，导致挂在同一个 CompanyId 下的客户互相可见
 *    （3001 一家就有 71 个客户、1373 台设备）—— 这是个越权漏洞。
 *    数据上 Production 其实等价于"是账单客户/Owner"（237/243 命中），与 Manager 无关。
 *
 * 注：UserTypeId=23（AquariusSalesSupport，仅 1 人）按本规则落到 operator。
 *     Aquarius 内部人员应该用 22，这一个账号要不要调整由业务定，不在这里特殊处理。
 */
export function deriveRoles(userTypeId) {
    if (userTypeId === USER_TYPE.AQUARIUS_SUPER_USER) return ['super'];
    if (userTypeId === USER_TYPE.CORPORATE_MANAGER) return ['manager'];
    return ['operator'];
}

/**
 * users.managed_company_ids 的**初始值**。
 *
 * ⚠️ 这一列是 **Pro 独有数据，增量同步永不覆盖它**（见 sync-simple.js 的
 *    ON CONFLICT DO UPDATE 子句里刻意不含此列）。SQL Server 里没有对应字段。
 *
 * 初始值：CorporateManager → [本人公司]；其余 → []
 *   生产数据上这 171 个 CorporateManager 恰好分布在 171 家不同公司（一家一个），
 *   所以这个默认值不会造成越权。
 *
 * 之后由 Pro 的界面维护。邮件里 Nick Duncan / Jay Dean 要管 HydroChem 的多家分公司
 * （VIC + NSW + QLD + WA + SA），只能人工配成 [2002, 2008, 2034, ...]。
 */
export function deriveManagedCompanyIds(userTypeId, companyId) {
    if (userTypeId === USER_TYPE.CORPORATE_MANAGER && companyId != null) return [companyId];
    return [];
}

// ─── 字段级映射 ──────────────────────────────────────────────────────────────

/**
 * users.jw_code ← UserInfomation.JWCode
 *
 * 邮件："Jiwa Code should be linked to Owner -> so maybe in userinfomation.
 *        Companies don't need a jiwa code."
 * 所以 JWCode 挂在 user 上，companies 表**没有** jw_code 列。
 */
export const deriveJwCode = (jwCode) => nullIfBlank(jwCode);

/** 老库各处 Logo 列的默认占位值，等于"没有 logo"。 */
export const DEFAULT_LOGO = 'YourLogoGoesToHere.JPG';

/**
 * companies.logo_url
 *
 * 2026-10-15 Hunter：老库 `UserInfomation.Logo` 实际是**该用户所在公司的 Logo**，
 * `Production = 1` 的用户大多就是公司 Owner（老站设备页按 Owner 取 Logo 显示，
 * 见 aquareporter BaseControllerController.GetLogoByUnitId）。所以优先取它，
 * 取不到再回退 `CompanyInfomation.CompanyLogoName`，都没有用占位值。
 *
 * ownerLogo 由查询给出：该公司 Production=1、Logo 非空且不是占位值的用户里，
 * 最近更新的那个（sync-simple.js 与 migration-company.js 用同一段 OUTER APPLY）。
 */
export function deriveCompanyLogo(ownerLogo, companyLogoName) {
    const owner = nullIfBlank(ownerLogo);
    if (owner && owner !== DEFAULT_LOGO) return owner;
    return nullIfBlank(companyLogoName) ?? DEFAULT_LOGO;
}

/**
 * controllers.model_type ← ControllerInfo.ControllerModel（生产物料型号）
 *
 * 做两件事：
 *
 * ① **去掉定长填充。** 源列是 `nchar(30)`，mssql 驱动把值原样带着尾随空格返回：
 *    `"ULTIMA3P                      "`。此前没处理，导致 PG 里 7980 行全是 30
 *    字符的填充值 —— `WHERE model_type = 'ULTIMA3P'` 一条都查不到，必须写成
 *    `btrim(model_type) = …`。这是 2026-09-18 发现并修的。
 *
 * ② **字符串 'NONE' → NULL。** 源库有 255 台的值是字面量 'NONE'（不是 NULL），
 *    那是"没填"的占位写法。照抄过来的话，PG 里就会出现一个叫 "NONE" 的合法型号，
 *    统计和下拉框都会把它当成真型号。真正的空值应该用 NULL 表达。
 *    源库另有 60 台本来就是 NULL，0 台是空字符串。
 *
 * ③ **统一成大写。** 源库同一个型号存在多种大小写写法：'Ultima3p' / 'Ultima3P'
 *    / 'ULTIMA3P' 是同一个型号的三种写法。SQL Server 的默认排序规则
 *    SQL_Latin1_General_CP1_CI_AS 大小写不敏感，把它们当成同一个值，所以旧站
 *    看不出问题；PG 区分大小写，照抄过来同一型号会在统计里裂成 2-3 行、下拉框
 *    出现重复项、按型号筛选漏数据。
 *
 *    2026-09-18 实查：13 组型号有大小写变体，共涉及 2964 台设备。
 *
 * ⚠️ 只认 'NONE'（忽略大小写）。'N/A' / '-' 这类占位符在生产数据里不存在
 *    （2026-09-18 实查），不做推测性处理 —— 真出现了应该先看清是什么再加规则。
 *
 * ④ **修正带空格的手误写法**，见下方 MODEL_ALIASES。7 个值、共 10 台。
 *
 * ⑤ **把非型号的占位文本归为 NULL**，见下方 MODEL_PLACEHOLDERS。
 */
/**
 * 带空格的手误写法 → 库里真实存在的型号。
 *
 * ⚠️ **不能统一用"去掉空格"处理**。2026-09-18 逐个核对：7 个带空格的值里，
 *    3 个去空格后能命中已有型号，4 个必须换成下划线，去空格反而会造出
 *    PANELULTIMA / ULTIMANP / X_PANULTIMA / PANELUCT 这 4 个库里不存在的新型号。
 *    右侧注释是该目标型号当前的设备数，即"这个映射确实落到了一个真型号上"的依据。
 */
const MODEL_ALIASES = {
    'PANEL UCT':    'PANEL_UCT',     //  13 台
    'PANEL ULTIMA': 'PANEL_ULTIMA',  // 131 台
    'X_PAN ULTIMA': 'X_PAN_ULTIMA',  // 122 台
    'ULTIMA NP':    'ULTIMA_NP',     // 600 台
    'ULTIMA 2P':    'ULTIMA2P',      // 329 台
    'ULTIMA 3P':    'ULTIMA3P',      // 809 台
    'ULTIMA 3P4G':  'ULTIMA3P4G',    // 257 台
};

/**
 * 不是型号的占位文本 → NULL。与 'NONE' 同理：它们表达的是"没填"，
 * 留在库里会变成看起来合法的型号，污染统计与下拉框。各 1 台。
 */
const MODEL_PLACEHOLDERS = new Set(['NONE', 'NOT USED', 'SALES DEMO', 'S/N USED BY SALES']);

export function deriveControllerModel(controllerModel) {
    const v = nullIfBlank(controllerModel);   // nullIfBlank 已含 trim，填充在这里被去掉
    if (v === null) return null;
    const upper = v.toUpperCase();
    if (MODEL_PLACEHOLDERS.has(upper)) return null;
    return MODEL_ALIASES[upper] ?? upper;
}

/**
 * controllers.spare_parts ← ControllerInfo.Serials（随机附带的部件序列号清单）
 *
 * 源列是一整段多行文本，一台设备通常 6-7 个序列号。分隔符不统一 ——
 * 2026-09-18 实查 1207 条有值的记录：换行 1071 条、空格 124 条、逗号 1 条。
 *
 * 此前直接照抄成一个字符串，结果是数据库客户端里显示成挤在一起的一长串，
 * 而且没法查"哪台设备装了序列号 X"。改成数组：
 *
 *   WHERE '2504-466653' = ANY(spare_parts)
 *
 * 按空白与逗号切分。序列号本身是字母数字加连字符（'2506-472510' /
 * '2507AP1037591' / 'S24007526'），内部不含空格，所以这样切是安全的。
 */
export function parseSerials(serials) {
    return classifySerials(serials).values;
}

/**
 * parseSerials 的完整结果，多带一个 isNote 标记 —— 同步时要把"被当成备注"的设备
 * UnitId 打出来，让人能去源库清理。规则本身仍然只有这一份。
 *
 * @return {{ values: string[], isNote: boolean }}
 */
export function classifySerials(serials) {
    const raw = nullIfBlank(serials);
    if (raw === null) return { values: [], isNote: false };

    const parts = raw.split(/[\s,]+/).map((x) => x.trim()).filter(Boolean);

    // ⚠️ 有人把这一列当备注用了。2026-09-18 实查 1204 台有值的设备：1183 台是干净的
    //    序列号清单，21 台写的是整句话，例如
    //      'SIM taken from SN602813 at 50 King st Tenant'
    //      'New panel replacing 606811 AM sent with MOD0000_PH_ORP'
    //
    //    那 21 条照切会碎成一堆单词，每个都假装是序列号。判据：真序列号一定含数字
    //    （'2506-472510' / '2507AP1037591' / 'S24007526' / '9336456000183'），
    //    只要有一个词不含数字，就当整段是备注、原样留作单个元素 —— 读得懂，
    //    也不会污染"哪台设备装了序列号 X"这类查询。
    if (parts.some((p) => !/[0-9]/.test(p))) return { values: [raw], isNote: true };

    return { values: parts, isNote: false };
}

/**
 * 从一段自由文本里抠出日期，用于 controller_service_logs / _tasks 的 occurred_on。
 *
 * 源数据是澳式 D/M/YY 或 D/M/YYYY（'25/6/26' = 2026-06-25）—— 服务人员习惯把日期
 * 直接写进正文（'Tech Advised - No Flow 25/6/26'）。
 *
 * ⚠️ **只在整段文字里恰好有一个完整日期时才返回**，其余一律 null：
 *
 *   'Tech Advised - Low ORP 3/9/26 + 4/9/26 + 7/9/26 + 9/9/26'  四个日期 → null
 *   '26/8 NB - per WA SM, we no longer have a contract'          缺年份   → null
 *
 * 一格里塞了四次处理记录，挑哪个当 occurred_on 都是猜。正文完整保留在 body 里，
 * 需要的人看得到；宁可留空，也不要写一个看起来精确、实际是猜的日期。
 *
 * @return {string|null} 'YYYY-MM-DD'
 */
export function parseNoteDate(text) {
    if (!text) return null;
    const all = [...String(text).matchAll(/\b(\d{1,2})\/(\d{1,2})\/(\d{2,4})\b/g)];
    if (all.length !== 1) return null;

    const [, d, m, y] = all[0];
    const day = Number(d);
    const mon = Number(m);
    let year = Number(y);
    if (year < 100) year += 2000;
    if (mon < 1 || mon > 12 || day < 1 || day > 31) return null;
    if (year < 2000 || year > 2100) return null;

    // 用 UTC 构造，避开本地时区把日期推前一天
    const dt = new Date(Date.UTC(year, mon - 1, day));
    // 回读校验，挡掉 31/2 这类不存在的日期
    if (dt.getUTCMonth() !== mon - 1 || dt.getUTCDate() !== day) return null;
    return dt.toISOString().slice(0, 10);
}

/** controllers.tags ← Suburb（"tag1, tag2" 逗号分隔） */
export function parseTags(suburb) {
    if (!suburb) return [];
    return String(suburb)
        .split(',')
        .map((t) => t.trim())
        .filter(Boolean);
}

// ─── 源库的两种时区约定 ──────────────────────────────────────────────────────

/**
 * ⚠️ **SQL Server 的 `datetime` 不带时区，而源库里两种约定并存。** 取"哪一天"
 * 的时候必须分开处理，否则有十个小时的窗口会把日期算错一天。
 *
 * 2026-09-22 实测（按小时统计落在 07-17 点的比例，业务时间应当占绝大多数）：
 *
 *   ControllerInfo.DateCreated      81%  ┐
 *   ControllerInfo.DateLastUpdate   70%  │ 存的是**布里斯班墙上时间**
 *   UserInfomation.DateCreated      80%  │ （服务器 GETDATE() 就是 UTC+10）
 *   CompanyInfomation.DateCreated   98%  │
 *   ControllerNotes.DateCreated     76%  ┘
 *   OperationLog.OperationTime       1%  ← 存的是**真 UTC**，峰值在 22-01 点
 *                                          （= 布里斯班 08-11 点，正是上班时间）
 *
 * 驱动（mssql/tedious，useUTC 默认 true）一律把值按 UTC 解释成 Date。于是：
 *   · 墙上时间列 → Date 里的 UTC 字段**就是**源库写的那个钟点，直接取日期
 *   · 真 UTC 列   → 要先加 10 小时才是布里斯班的日期
 *
 * 布里斯班常年 UTC+10，没有夏令时，所以是个常数、不需要 Intl。
 */
const BRISBANE_UTC_OFFSET_MS = 10 * 60 * 60 * 1000;

/** 真 UTC 时刻（OperationLog.OperationTime）→ 布里斯班那天的 'YYYY-MM-DD'。 */
export function utcInstantToBrisbaneDate(value) {
    const d = toDate(value);
    return d === null ? null
        : new Date(d.getTime() + BRISBANE_UTC_OFFSET_MS).toISOString().slice(0, 10);
}

/** 墙上时间（ControllerInfo.DateCreated 等）→ 'YYYY-MM-DD'，不做偏移。 */
export function wallClockToDate(value) {
    const d = toDate(value);
    return d === null ? null : d.toISOString().slice(0, 10);
}

/**
 * 墙上时间列 → 真正的时刻（Date）。
 *
 * 驱动把布里斯班墙上时间当 UTC 读进来，所以得到的 Date 比真实时刻**快 10 小时**。
 * 要和应用写的真 UTC（Pro 的 `NOW()`）比较先后时必须先减回去 ——
 * 例如 users.last_login_at 取两边较晚值（见 upserts.js UPSERT_USERS）。
 */
export function wallClockToInstant(value) {
    const d = toDate(value);
    return d === null ? null : new Date(d.getTime() - BRISBANE_UTC_OFFSET_MS);
}

function toDate(value) {
    if (!value) return null;
    const d = value instanceof Date ? value : new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
}

// ─── 内部备注与后台操作日志 ──────────────────────────────────────────────────

/**
 * admin_controller_notes ← ControllerInfo.Notes（内部备注，按 '-->' 拆成多行）
 *
 * 源列是一格里塞了一台设备全部历史的自由文本，段与段之间用 `-->` 分隔。
 * 2026-09-22 实测：7409 台有备注，切出 12899 段，其中只有 6744 段（6269 台）是
 * 人写的正文。
 * 剩下的都要丢，理由分三类：
 *
 * ① **逐键存盘噪音（1236 段）** —— 旧站编辑 SIMCardNo 时每敲一个字符就往
 *    Notes 里追一段，于是留下
 *      'SIMCardNo updated to: 8' → '…: 80' → '…: 800' → '…: 8000' → …
 *    一台设备能有 20 多段这种。它们不是备注，是旧站的 bug 现场。
 *
 * ② **字段变更回显（'->[X changed to Y]'）** —— 与 OperationLog 记的是同一件事，
 *    而 OperationLog 有操作人和准确时间，这里没有。剥掉，不重复存。
 *
 * ③ **剥完只剩空白的段（4875 段）** —— ①② 剥干净后什么都不剩的。
 *
 * 开头的 'New Controller' 是建档时自动写的固定前缀，同样剥掉。
 *
 * ⚠️ 返回的是**过滤后的稠密序号**：第 i 段就是 source_seq = i。新备注一律追加在
 *    末尾，所以已有段的序号不会因为追加而移动 —— 这是同步能按
 *    (source, source_id, source_seq) 幂等 UPSERT 的前提。
 *
 * @return {string[]} 每段一个元素，已 trim，不含空串
 */
export function parseControllerNotes(notes) {
    const raw = nullIfBlank(notes);
    if (raw === null) return [];

    const out = [];
    for (const piece of String(raw).split('-->')) {
        const body = piece
            .replace(/->\[[^\]]*\]/g, ' ')      // ② 字段变更回显
            .replace(/^New Controller/i, ' ')   // 建档固定前缀
            .trim();
        if (!body) continue;                                          // ③
        if (/^SIMCardNo (?:updated|changed) to\b/i.test(body)) continue;  // ①
        if (/^\w+ changed to\b/i.test(body)) continue;                // ② 的漏网写法
        // ③ 的一种：只剩标点。源数据里有的 '->[…]' 块**内部就带着 '-->'**
        // （逐键存盘的噪音被包进了括号），先按 '-->' 切就把括号撑碎了，
        // 尾巴会留下一个孤零零的 ']'（实测 46 段）。一个字母数字都没有的段
        // 一律不要 —— 它不可能是人写的备注。
        if (!/[\p{L}\p{N}]/u.test(body)) continue;
        out.push(body);
    }
    return out;
}

/**
 * controller_subscriptions.paid_by_user_id 的兜底账户。
 *
 * UserId 5040 = 'Spare Parts' / 'Aquarius Corp Acc'（CompanyId 3001，UserTypeId 24），
 * 也就是公司自己的现金账户。设备没有付费用户时订阅记在它名下 —— 留 NULL 的话
 * "这笔钱是谁付的"就永远查不出来，而这张表以后要接用户付费记录。
 *
 * ⚠️ 2026-09-22 实测：379 台有 Renewal 记录的设备**全部**有 AccManagerId，
 *    所以迁过来的 legacy 行一条都用不上这个兜底；它是给以后新录入用的。
 */
export const CASH_ACCOUNT_USER_ID = 5040;

// ─── 后台操作审计（admin_operation_logs）────────────────────────────────────

/**
 * 迁移哪些 OperationType，以及它们改的是什么对象。
 *
 * 口径是"**adminpage 里对设备和用户的修改**，外加续费"：一行只回答
 * 「谁 · 什么时候 · 把哪个字段 · 改成了什么」。因此以下类型**整个不迁**：
 *
 *   LOGIN / LOGINTOREPORT      登录事件，且 UpdatedRecord 装的是 8 小时有效的
 *                              登录 JWT 原文 —— 每天同步等于把还能用的 bearer
 *                              token 复制进一张随便谁都能查的审计表
 *   ADD_CONTROLLER / ADD_USER  整条新增，没有"改成什么"可记。全展开是
 *                              1537 条 × 几十列 ≈ 6 万行建档噪音
 *   RELEASE_CONTROLLER         UpdatedRecord 只有一个 UnitId，没有字段改动
 *   ADD_ACCESSORY /            配件出入库，不属于"设备和用户的修改"
 *   UPDATE_ACCESSORY
 */
const OPERATION_LOG_TYPES = {
    MODIFY_CONTROLLER:   'controller',
    ACTIVE_CONTROLLER:   'controller',
    DEACTIVE_CONTROLLER: 'controller',
    UPDATE_RENEWAL:      'controller',
    MODIFY_USER:         'user',
    RESETPASSWORD_USER:  'user',
};

/** admin_operation_logs.target_type 的合法取值，建表的 CHECK 用同一份。 */
export const OPERATION_TARGET_TYPES = Object.freeze(
    [...new Set(Object.values(OPERATION_LOG_TYPES))],
);

/**
 * 不记的列，两个理由：
 *
 * ① **记账列** —— 每次编辑都会变，进了审计表只会把真正的改动淹掉
 *    （`DateLastUpdate` 在 4461 条 MODIFY_CONTROLLER 里条条都在变）。
 *
 * ② **`Password`（明文密码列）** —— mapping.json 已定为死列不迁移；而且它总是
 *    和 `PasswordHash` 一起变，两行说的是同一件事（“密码被改了”）。留
 *    `password_hash` 那一行（值已抹成 '***'）就够了，82 条重置记录因此从
 *    160 行降到 80 行。
 */
const OPERATION_SKIP_FIELDS = new Set([
    'DateLastUpdate', 'LastUpdateDate', 'UserIdForLastChange', 'Password',
]);

/** 值要抹掉的列：记"改过密码"这件事，不记密码本身。 */
const OPERATION_MASKED_FIELDS = new Set([
    'password', 'passwordhash', 'newpassword', 'confirmpassword',
]);

/**
 * 源列名 → PG 列名。
 *
 * ⚠️ 这是 mapping.json 决策的**第五处副本**（前四处：initDatabase / upserts /
 *    sync-simple / migration-controller）。放在这里是因为审计表记的是"哪个字段
 *    被改了"，写旧列名会让同一张表里出现两套词汇 —— 迁过来的行写 `SiteLocation`、
 *    以后 Pro 写进来的行写 `creator`，查询就得两个都写。
 *
 *    表里**没有**的列保持源列名原样（`SetupVersionId` / `PlanType` / `Production`
 *    这些 mapping.json 标了不迁的列，照样会出现在历史改动里 —— 那是当年真的改过
 *    的东西，不能因为现在不迁就假装没发生）。
 */
const OPERATION_FIELD_RENAMES = {
    controller: {
        UnitId: 'unit_id', SerialNo: 'serial_no', SIMCardNo: 'sim_card_no',
        AccManagerId: 'owner_user_id', CompanyId: 'company_id', SystemID: 'system_id',
        FirmwareVersion: 'system_type', ControllerModel: 'model_code',
        LinuxTimeZoneId: 'timezone_id', ProductionStatus: 'production_status',
        Activated: 'activated', DateCreated: 'created_at',
        SiteLocation: 'creator', Suburb: 'tags', Barcode: 'bar_code',
        CustPO: 'po_number', Serials: 'accessory_serial_numbers',
        Notes: 'notes', Action: 'action',
        RenewalInv: 'invoice_no', RenewalDate: 'ends_on',
    },
    user: {
        UserId: 'user_id', CompanyId: 'company_id', Email: 'email', Mobile: 'mobile',
        IsPhoneVerified: 'mobile_verified', UserName: 'user_name', Name: 'name',
        PasswordHash: 'password_hash', Logo: 'avatar_url', JWCode: 'jw_code',
        UserTypeId: 'roles', Activated: 'activated',
        RequiresTwoFactorAuth: 'mfa_enabled', LastLoginTime: 'last_login_at',
        DateCreated: 'created_at',
    },
};

/** `{"Notes": {"old": …, "new": …}, …}` 这种"adminpage 自己算好的 diff"格式？ */
function isDiffFormat(o) {
    return o !== null && typeof o === 'object' && !Array.isArray(o)
        && Object.keys(o).length > 0
        && Object.values(o).every((v) => v !== null && typeof v === 'object'
            && !Array.isArray(v) && 'old' in v && 'new' in v);
}

function parseJson(text) {
    const raw = nullIfBlank(text);
    if (raw === null || raw === '-') return null;
    try {
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

/**
 * 字段值 → admin_operation_logs.new_value 的 TEXT。null 原样传（表示被清空）。
 *
 * 整点午夜的 UTC 时间戳只保留日期部分：`RenewalDate` 这类本来就是日期，
 * 存成 '2026-08-13T00:00:00.000Z' 只会让人以为那个 00:00 有意义。非整点的
 * （`DateCreated` 等）原样保留，不碎掉信息。
 */
const MIDNIGHT_ISO = /^(\d{4}-\d{2}-\d{2})T00:00:00(?:\.000)?Z$/;

function formatFieldValue(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'string') {
        const date = v.match(MIDNIGHT_ISO);
        return date ? date[1] : nullIfBlank(v);
    }
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
}

/**
 * OperationLog 的一行 → admin_operation_logs 的**若干行**（一个改动字段一行）。
 *
 * ── 源库有两种格式，都要认 ───────────────────────────────────────────────────
 *
 * **A（2025-05-08 起，3586 条）** adminpage 自己就在写字段级 diff：
 *     OriginalRecord = {"Serials":{"old":null,"new":"2504-466670"}, …}
 *     UpdatedRecord  = {"UnitId":606726}
 *   直接取，不用比对。
 *
 * **B（2025-05-08 以前，1221 条）** 前后各存一份整行快照（47-52 列）：
 *     OriginalRecord = {UnitId:…, SerialNo:…, …}   UpdatedRecord = 同样的列
 *   逐列比出差异。
 *
 * 另外两类不走 diff：
 *   · ACTIVE_ / DEACTIVE_CONTROLLER（49 条）—— OriginalRecord 是 '-'，没有快照。
 *     但操作类型本身就说明了改动：activated 改成 true / false，据此直接造一行。
 *   · UPDATE_RENEWAL（74 条）—— OriginalRecord 是 {RenewalInv, RenewalDate}，
 *     UpdatedRecord 是 {UnitIds:[…], updated:N}，一次改多台。**按台拆开**，
 *     每台每个字段一行，否则"这台设备的到期日被谁改过"还是查不了。
 *
 * @param {{LogId:number, UserId:number, OperationType:string,
 *          OriginalRecord:string, UpdatedRecord:string, OperationTime:Date}} row
 * @return {Array<{source_id:number, source_seq:number, operation_type:string,
 *                 actor_user_id:number|null, target_type:string, target_id:number,
 *                 field:string|null, new_value:string|null, occurred_at:Date}>}
 *         不迁移的类型返回空数组
 */
export function deriveOperationLogRows(row) {
    const type = nullIfBlank(row.OperationType);
    const targetType = type === null ? undefined : OPERATION_LOG_TYPES[type];
    if (targetType === undefined) return [];

    const original = parseJson(row.OriginalRecord);
    const updated = parseJson(row.UpdatedRecord);
    const renames = OPERATION_FIELD_RENAMES[targetType] ?? {};

    const base = {
        source_id:     row.LogId,
        operation_type: type,
        actor_user_id: row.UserId ?? null,
        target_type:   targetType,
        occurred_at:   row.OperationTime ?? null,
    };
    const out = [];
    const push = (targetId, field, value) => {
        if (targetId === null || targetId === undefined) return;
        out.push({
            ...base,
            source_seq: out.length,
            target_id:  Number(targetId),
            field:      field === null ? null : (renames[field] ?? field),
            new_value:  field !== null && OPERATION_MASKED_FIELDS.has(field.toLowerCase())
                ? '***'
                : formatFieldValue(value),
        });
    };

    // ── 激活 / 停用：类型本身就是改动内容 ────────────────────────────────────
    if (type === 'ACTIVE_CONTROLLER' || type === 'DEACTIVE_CONTROLLER') {
        const unit = pickId(updated, 'UnitId') ?? pickId(original, 'UnitId');
        push(unit, 'Activated', type === 'ACTIVE_CONTROLLER');
        return out;
    }

    // ── 续费：一条日志改多台，按台拆 ────────────────────────────────────────
    if (type === 'UPDATE_RENEWAL') {
        const units = Array.isArray(updated?.UnitIds) ? updated.UnitIds : [];
        for (const unit of units) {
            for (const field of ['RenewalInv', 'RenewalDate']) {
                if (original?.[field] === undefined) continue;
                push(unit, field, original[field]);
            }
        }
        return out;
    }

    const target = pickId(updated, targetType === 'user' ? 'UserId' : 'UnitId')
        ?? pickId(original, targetType === 'user' ? 'UserId' : 'UnitId');

    // ── 格式 A：adminpage 已经算好的 diff ───────────────────────────────────
    if (isDiffFormat(original)) {
        for (const [field, { old, new: next }] of Object.entries(original)) {
            if (OPERATION_SKIP_FIELDS.has(field)) continue;
            if (JSON.stringify(old) === JSON.stringify(next)) continue;
            push(target, field, next);
        }
        return out;
    }

    // ── 格式 B：前后两份整行快照，逐列比 ────────────────────────────────────
    if (original !== null && updated !== null
        && typeof original === 'object' && typeof updated === 'object') {
        for (const [field, next] of Object.entries(updated)) {
            if (OPERATION_SKIP_FIELDS.has(field)) continue;
            if (JSON.stringify(original[field]) === JSON.stringify(next)) continue;
            push(target, field, next);
        }
    }
    return out;
}

/** 从 JSON 里取一个整数 id，取不到返回 null。 */
function pickId(obj, key) {
    const v = obj === null || typeof obj !== 'object' ? undefined : obj[key];
    if (Number.isInteger(v)) return v;
    if (typeof v === 'string' && /^\d+$/.test(v.trim())) return Number(v.trim());
    return null;
}

/**
 * 从 OperationLog 反推**每段设备备注是谁、什么时候写的**。
 *
 * ControllerInfo.Notes 那一列本身不记作者。但 adminpage 每次改备注都会在
 * OperationLog 里留下改前改后的全文，把新出现的那一段认出来，就知道是谁加的。
 *
 * ⚠️ **只能覆盖 16.8%**（2026-09-22 实测 1132/6744 段，10 位操作人）。
 *    OperationLog 最早只到 2024-07，而备注能追到 2015 —— 更早的段没有任何地方
 *    记过作者，只能留空。宁可留空，也不要拿"设备 owner"之类的近似值顶上：
 *    备注是产线和后台人员写的，owner 是客户侧的客户经理，填了就是错的。
 *
 * 同一段文字在多条日志里出现时取**最早**那条 —— 那才是"写它的人"，
 * 后面的日志只是又把整段存了一遍。
 *
 * @param {Array<{LogId, UserId, OperationTime, OriginalRecord, UpdatedRecord}>} rows
 *        只需要 OperationType='MODIFY_CONTROLLER' 且正文里出现过 Notes 的那些
 * @return {Map<string, {userId:number|null, at:Date}>}  键是 `${unitId}|${片段}`
 */
export function deriveNoteAuthors(rows) {
    const out = new Map();
    for (const r of rows) {
        const original = parseJson(r.OriginalRecord);
        const updated = parseJson(r.UpdatedRecord);

        let unit = null;
        let before = null;
        let after = null;
        if (isDiffFormat(original)) {
            unit = pickId(updated, 'UnitId');
            if (original.Notes) { before = original.Notes.old; after = original.Notes.new; }
        } else if (original !== null && updated !== null
                   && typeof original === 'object' && typeof updated === 'object') {
            unit = pickId(updated, 'UnitId') ?? pickId(original, 'UnitId');
            before = original.Notes;
            after = updated.Notes;
        }
        if (unit === null || after === null || after === undefined) continue;

        const existing = new Set(parseControllerNotes(before));
        for (const body of parseControllerNotes(after)) {
            if (existing.has(body)) continue;          // 这一段本来就有，不是这次加的
            const key = `${unit}|${body}`;
            if (out.has(key)) continue;                // 已有更早的一条
            out.set(key, { userId: r.UserId ?? null, at: r.OperationTime ?? null });
        }
    }
    return out;
}

// ─── 通信方式 ────────────────────────────────────────────────────────────────

/**
 * controllers.comm_types 数组里的取值。
 *
 * 前五个与 adminpage 前端 config/defaultSettings.ts 的 commOption 一一对应：
 *
 *   SQL Server SIMCardNo   界面 label            PG comm_type
 *   'Wifi'                 Wifi                  wifi
 *   'Ethernet'             Ethernet              ethernet
 *   数字开头（真实卡号）     Modem & SIM           sim
 *   'NOSIMCARD'            Modem & No Sim Card   modem_no_sim
 *   'None' / 空            None                  none
 *   历史脏值                ——                    other   ← 第六个，见下
 */
export const COMM_TYPE = {
    WIFI: 'wifi',
    ETHERNET: 'ethernet',
    SIM: 'sim',
    MODEM_NO_SIM: 'modem_no_sim',
    NONE: 'none',
    OTHER: 'other',
};

/** 供 initDatabase.js 拼 CHECK 约束用 —— 取值清单只有这一份。 */
export const COMM_TYPE_VALUES = Object.values(COMM_TYPE);

/**
 * 互斥值：出现了就必须是数组里**唯一**的元素。
 *
 *   none  —— "无通信"和"有 WiFi"不可能同时成立
 *   other —— "没分类出来"是对整台设备的判断，不能和确定的方式并列
 *
 * initDatabase.js 的 CHECK 约束按这个清单生成，两处不会漂。
 */
export const COMM_TYPE_EXCLUSIVE = [COMM_TYPE.NONE, COMM_TYPE.OTHER];

/**
 * 把 SQL Server 的 dbo.ControllerInfo.SIMCardNo 一列拆成 **comm_types + sim_card_no**。
 *
 * ── 为什么要拆 ──────────────────────────────────────────────────────────────
 *
 * 老库把"通信方式"和"SIM 卡号"挤在同一列里。adminpage 的 Communication mode
 * 下拉框选 Ethernet，前端就把字符串 'Ethernet' 原样写进 SIMCardNo
 * （adminpage-web/src/pages/ControllerList/components/Edit.tsx 的 onChange:
 *  `form.setFieldsValue({ SIMCardNo: form.getFieldValue('SimCardType') })`）。
 * 于是任何想按通信方式统计的代码都得做字符串嗅探。2026-10 起的通信套餐按
 * Ethernet / WiFi 划线收费，判据不能是嗅探出来的。
 *
 * ── 规则：包含式匹配，可叠加 ────────────────────────────────────────────────
 *
 *   ① 数字开头                → ['sim']，原值留在 sim_card_no
 *   ② 归一化后包含 'wifi'     → 追加 'wifi'
 *   ③ 包含 'eth'              → 追加 'ethernet'
 *   ④ 包含 'sim' 或 'modem'   → 追加 'modem_no_sim'
 *   ⑤ 以上都没命中：空 / 'none' → ['none']，其余 → ['other']
 *
 * ②③④ 是**累加**的，所以一台设备可以同时具备多种通信方式 —— 生产数据实测有
 * 6 台：'OwnSimandEth,'(4) → {ethernet,modem_no_sim}，'NOSIMCARD,WIFI'(1) 与
 * 'NowWifiPrevModem'(1) → {wifi,modem_no_sim}。
 *
 * 归一化 = 转小写 + 去掉所有非字母数字字符，用来吃掉历史写法变体
 * （'WiFi Upgrade29Aug18' / 'NOSIMCARD,' / 'Modem & No Sim Card'）。
 *
 * ⚠️ ① 必须排在最前。2026-09-16 实查 AquariusDev：有 1899 个 distinct 值是数字
 *    开头且含字母（'8000415794579N,0477183421' 这种卡号带后缀和联系电话），共
 *    1994 台；其中只有 1 个含关键字（'80002006notfullsim'，本身就是 SIM）。
 *    所以数字开头一律判 sim 是安全的；反过来先跑关键字匹配会把近 2000 台判错。
 *
 * ⚠️ 'none' 与 'other' 不能合并：
 *      none  = 产线明确选了"无通信"        → 本来就不该有套餐
 *      other = 老数据看不出来，需人工确认  → 缺套餐报表必须列出来
 *    两者在 controllers_missing_subscription 上的处置完全相反。
 *
 * ⚠️ "数字开头 = 真实卡号"这条判据在三处各写了一份（本函数、前端 Edit.tsx、
 *    后端 analysisController.classifyCommType）。改一处就要改三处。
 *
 * @param  {string|null} simCardNo  dbo.ControllerInfo.SIMCardNo（NChar(50)，带尾随空格）
 * @return {{ commTypes: string[], simCardNo: string|null }}
 */
export function deriveCommType(simCardNo) {
    const raw = nullIfBlank(simCardNo);

    if (raw && /^\d/.test(raw)) {
        return { commTypes: [COMM_TYPE.SIM], simCardNo: raw };
    }

    // 走到这里说明这一列存的不是卡号，sim_card_no 一律为 NULL。
    const n = (raw ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');

    const types = [];
    if (n.includes('wifi')) types.push(COMM_TYPE.WIFI);
    if (n.includes('eth')) types.push(COMM_TYPE.ETHERNET);
    if (n.includes('sim') || n.includes('modem')) types.push(COMM_TYPE.MODEM_NO_SIM);

    if (types.length > 0) return { commTypes: types, simCardNo: null };

    return {
        commTypes: [!n || n === 'none' ? COMM_TYPE.NONE : COMM_TYPE.OTHER],
        simCardNo: null,
    };
}

/**
 * 仍落在 'other' 的历史值 —— 342 台（2026-09-16 实查）。包含式匹配已经把
 * 'DaveTestWiFi' / 'OwnSIM' / 'ModemUpgrade' 这些自动归位了，剩下的是**确实
 * 看不出通信方式**的流程标记：
 *
 *   'NonWeb'        240 台   "不联网"，等同 none 还是另一种状态？
 *   'Factory'        44 台   产线设备
 *   'WebUpg' 系列    33 台   'WebUpgJan23' / 'webupg210226' … 升级标记
 *   'SalesDemo' / 'slsdemo' / 'Unknown'      非客户设备
 *   'usedBy602824' / 'repwith602636' / …     设备流转备注
 *
 * 这些要不要计费是商务判断，不猜。缺套餐报表会把它们列出来等人工认领；
 * 认领完这里的台数应当单调下降，可以直接当数据质量进度条。
 */
export const COMM_TYPE_PENDING_REVIEW = Object.freeze({
    nonWeb: ['nonweb'],
    webUpgrade: ['webupg'],
    testRig: ['factory', 'salesdemo', 'slsdemo', 'unknown'],
    deviceNotes: ['usedby', 'repwith', 'progas'],
});

/**
 * user_controller_assignments.permission ← AccessLevel
 *   0 → READONLY，其余 → NORMAL
 *
 * AccessLevel=2（Corporate Manager）在老站从不落库，生产数据核实过只有 0 和 1
 * （READONLY 2466 / NORMAL 16200），所以这里没有对应分支。
 */
export function mapPermission(accessLevel) {
    return accessLevel === 0 ? PERMISSION.READONLY : PERMISSION.NORMAL;
}

/**
 * users.name —— **显示名**，取 UserInfomation.Name（回退 UserName → Email）。
 *
 * 2026-10-15 前是 UserName 优先，结果 Pro 改了显示名（写老库 Name）会被同步改回
 * UserName（project-docs 06 W2）。老站里两者语义不同：Name 是显示名，UserName 是
 * 登录名（老站按 CompanyId + UserName 登录，LoginController.cs:303），后者现在单独
 * 进 users.user_name。
 */
export const deriveDisplayName = (row) =>
    nullIfBlank(row.Name) ?? nullIfBlank(row.UserName) ?? row.Email;

/** users.user_name ← UserInfomation.UserName（老站登录名，公司内唯一，可空）。 */
export const deriveLoginName = (row) => nullIfBlank(row.UserName);

/** users.mobile 的默认值，与老站 ControllerController.AddUser 一致。 */
export const deriveMobile = (mobile) => nullIfBlank(mobile) ?? '0000000000';

// ─── 去重 ────────────────────────────────────────────────────────────────────

/**
 * 按 lower(email) 去重 —— users 上有唯一索引 users_email_lower_idx。
 *
 * 保留优先级（沿用 sync-simple.js 原有逻辑）：
 *   Activated=1 > UserTypeId=24 > 较大的 UserId
 *
 * 返回 { rows, dropped }，dropped 用于打日志。**不抛异常** —— 同步是无人值守跑的，
 * 中止会让整个同步停摆；一次性迁移也用同一套逻辑，两边才能对得上。
 */
export function dedupeUsersByEmail(sourceRows) {
    const byEmail = new Map();
    const dropped = [];

    const better = (candidate, existing) => {
        const cAct = toBool(candidate.Activated);
        const eAct = toBool(existing.Activated);
        if (cAct !== eAct) return cAct;

        const c24 = candidate.UserTypeId === USER_TYPE.CORPORATE_MANAGER;
        const e24 = existing.UserTypeId === USER_TYPE.CORPORATE_MANAGER;
        if (c24 !== e24) return c24;

        return candidate.UserId > existing.UserId;
    };

    for (const row of sourceRows) {
        const key = (row.Email || '').toLowerCase().trim();
        if (!key) continue;

        const existing = byEmail.get(key);
        if (!existing) {
            byEmail.set(key, row);
            continue;
        }
        if (better(row, existing)) {
            byEmail.set(key, row);
            dropped.push({ email: key, keptUserId: row.UserId, droppedUserId: existing.UserId });
        } else {
            dropped.push({ email: key, keptUserId: existing.UserId, droppedUserId: row.UserId });
        }
    }

    return { rows: [...byEmail.values()], dropped };
}

/**
 * 按自然键 (UserId, UnitId) 合并授权行 —— 目标表有 UNIQUE (user_id, unit_id)，
 * 而源表没有唯一约束。
 *
 * 合并策略（docs/migration/03 的 O8 建议值）：
 *   permission   取最高（任一行是 NORMAL 就算 NORMAL）
 *   send_*_alarm 取 OR（任一行开了就算开）
 *
 * 比旧同步脚本的"取 SeqKey 最大的一条"好 —— 后者会静默丢掉其它行的告警偏好。
 * 2026-09-08 核实生产数据里 18,666 行恰好是 18,666 个唯一组合，当前无重复，
 * 这段逻辑是防御性的。
 *
 * ⚠️ SeqKey 刻意不参与 —— PG 的 id 由 SERIAL 管理，不再直填 SeqKey（O6/R-007）。
 */
export function mergeAssignmentsByPair(sourceRows) {
    const byPair = new Map();
    let merged = 0;

    for (const row of sourceRows) {
        const key = `${row.UserId}|${row.UnitId}`;
        const permission = mapPermission(row.AccessLevel);
        const sms = toBool(row.SendSMSAlarm);
        const email = toBool(row.SendEmailAlarm);

        const existing = byPair.get(key);
        if (!existing) {
            byPair.set(key, {
                UserId: row.UserId,
                UnitId: row.UnitId,
                permission,
                sendSmsAlarm: sms,
                sendEmailAlarm: email,
            });
            continue;
        }
        merged++;
        existing.permission = Math.max(existing.permission, permission);
        existing.sendSmsAlarm = existing.sendSmsAlarm || sms;
        existing.sendEmailAlarm = existing.sendEmailAlarm || email;
    }

    return { rows: [...byPair.values()], merged };
}
