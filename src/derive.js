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

/** controllers.tags ← Suburb（"tag1, tag2" 逗号分隔） */
export function parseTags(suburb) {
    if (!suburb) return [];
    return String(suburb)
        .split(',')
        .map((t) => t.trim())
        .filter(Boolean);
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

/** users.name 的回退链：UserName → Name → Email */
export const deriveUserName = (row) => row.UserName || row.Name || row.Email;

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
