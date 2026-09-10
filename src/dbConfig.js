/**
 * dbConfig.js —— 连接串解析，唯一一份。
 *
 * 此前 `parseSqlServerUrl` 在 5 个文件里各有一份拷贝（sync-simple + 4 个
 * migration），跟 derive.js 要解决的是同一类问题：同一份逻辑抄多遍，改一处
 * 就漂一处。
 *
 * ── 为什么要剥引号（2026-09-09 实测踩到的坑）──────────────────────────────
 *
 * `.env` 里的值是写成 `SQL_SERVER_URL="sqlserver://…;trustServerCertificate=true"`
 * 的。`dotenv` 会自动剥掉外层引号，所以**本地跑一切正常**。
 *
 * 但部署时凭据是用 `docker run -e SQL_SERVER_URL='…'` 注入的，引号会原样进到
 * 变量值里。于是连接串最后一段变成 `trustServerCertificate=true"`，
 * `v === 'true'` 判为 false，TLS 信任被静默关掉，然后报一个跟引号毫无关系的
 * 错误：
 *
 *     Failed to connect to aquareporter.com.au:1433 - self-signed certificate
 *
 * 谁从 .env 复制值去配 Azure Pipelines 变量，就会中这一枪，而且错误信息完全
 * 指不到真正的原因。所以这里主动剥掉外层引号，并打一条 warning 让人知道
 * 变量配歪了。
 */

/** 剥掉整个值外层成对的引号；剥掉了就返回 stripped=true 供调用方告警。 */
function unquote(raw) {
    const s = String(raw ?? '').trim();
    const m = s.match(/^(["'])([\s\S]*)\1$/);
    return m ? { value: m[2].trim(), stripped: true } : { value: s, stripped: false };
}

let warnedSql = false;
let warnedPg = false;

/**
 * 解析 `sqlserver://host:1433;database=…;user=…;password=…;trustServerCertificate=true`
 * 成 mssql 的 config 对象。
 */
export function parseSqlServerUrl(url = '') {
    const { value, stripped } = unquote(url);
    if (stripped && !warnedSql) {
        console.warn(
            '⚠ SQL_SERVER_URL 的值带着外层引号，已自动剥掉。' +
            '请把环境变量里的引号去掉 —— 留着它会让 trustServerCertificate ' +
            '等末位参数解析错误（表现为 self-signed certificate 连接失败）。',
        );
        warnedSql = true;
    }

    const clean = value.replace(/^sqlserver:\/\//i, '').replace(/^["']/, '').trim();
    const cfg = {
        options: { encrypt: true, trustServerCertificate: true, enableArithAbort: true },
        pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    };

    const hostMatch = clean.match(/^([^;:]+):?(\d+)?/);
    if (hostMatch) {
        cfg.server = hostMatch[1];
        if (hostMatch[2]) cfg.port = parseInt(hostMatch[2], 10);
    }

    for (const part of clean.split(';').slice(1)) {
        const eq = part.indexOf('=');
        if (eq < 1) continue;
        const k = part.slice(0, eq).trim().toLowerCase();
        // 末位参数可能残留引号（值里含 `;` 时也可能），一并剥掉
        const v = part.slice(eq + 1).trim().replace(/^["']|["']$/g, '');
        if (k === 'database')                    cfg.database = v;
        else if (k === 'user')                   cfg.user = v;
        else if (k === 'password')               cfg.password = v;
        else if (k === 'trustservercertificate') cfg.options.trustServerCertificate = v === 'true';
        else if (k === 'encrypt')                cfg.options.encrypt = v !== 'false';
    }

    return cfg;
}

/**
 * 启动时校验必需的连接串都在。
 *
 * ⚠️ 为什么需要这个：容器化部署时 .env 是从宿主机 bind-mount 进来的
 * （`./.env:/app/.env`）。文件缺失、或者宿主机权限不允许容器里的 uid 读取时，
 * `dotenv.config()` **不抛异常**，只是安静地什么都不做。于是连接串是 undefined，
 * 最终表现成 `Failed to connect to undefined:1433` 之类看不懂的错误 ——
 * 而真正的原因是一个文件没挂上。
 *
 * 所以这里主动检查并给出可操作的提示。
 */
function requireEnv(name) {
    const v = String(process.env[name] ?? '').trim();
    if (v) return v;
    throw new Error(
        `缺少环境变量 ${name}。\n` +
        `  · 本地运行：确认项目根目录有 .env 且包含 ${name}=…\n` +
        `  · 容器运行：确认宿主机的 .env 已挂到 /app/.env，且**容器内的用户读得到**\n` +
        `    （镜像以 uid 1001 运行；宿主机文件建议 chmod 644，靠父目录 700 做隔离）。\n` +
        `    自查：docker exec <容器> sh -c 'ls -l /app/.env && head -c 20 /app/.env'`,
    );
}

/**
 * 从环境变量取 SQL Server 配置。
 *
 * @returns {{cfg: object, srcDb: string}} srcDb 是三段式表名用的库名 ——
 *   **必须从连接串的 database= 取，不要硬编码**。查询写的是
 *   `<srcDb>.dbo.<table>`，写死库名的话改 .env 指向别的库根本不生效、
 *   而且不报错（2026-09-09 修）。
 */
export function getSqlServerConfig() {
    const cfg = parseSqlServerUrl(requireEnv('SQL_SERVER_URL'));
    return { cfg, srcDb: cfg.database || 'AquariusEmailDB' };
}

/**
 * PostgreSQL 连接串。同样剥引号 —— `pg` 遇到带引号的连接串会直接解析失败，
 * 错误信息也很难指向真正的原因。
 */
export function getPostgresUrl() {
    const { value, stripped } = unquote(requireEnv('POSTGRESQL_URL'));
    if (stripped && !warnedPg) {
        console.warn('⚠ POSTGRESQL_URL 的值带着外层引号，已自动剥掉。请去掉环境变量里的引号。');
        warnedPg = true;
    }
    return value;
}
