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

/** controllers.tags ← Suburb（"tag1, tag2" 逗号分隔） */
export function parseTags(suburb) {
    if (!suburb) return [];
    return String(suburb)
        .split(',')
        .map((t) => t.trim())
        .filter(Boolean);
}

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
