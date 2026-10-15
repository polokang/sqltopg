/**
 * syncLog.js —— 把每次同步运行记进 PG 的 sync_runs 表。
 *
 * ⚠️ 两条设计约束，都不是可选的：
 *
 * 1. **必须用独立连接、在同步事务之外提交。**
 *    sync-simple.js 把四张表的同步包在一个事务里；如果日志也写在那个事务里，
 *    同步失败回滚时会把自己那条失败记录一并回滚 —— 失败就永远不留痕，
 *    正好在最需要日志的时候没有日志。
 *    这里全部走 `pool.query()`（从连接池另取一个 client），与事务无关。
 *
 * 2. **日志写失败绝不能让同步失败。**
 *    日志是观测手段，不是业务。所有函数内部吞掉异常、只打 console.error ——
 *    因为记不上日志而让一次本来成功的同步报错，是本末倒置。
 *
 * 表结构见 src/initDatabase.js 第 11 节（该表不在 init-db 的 DROP 列表里，
 * 重建镜像表不会清掉同步历史）。
 */

import os from 'os';

/**
 * 运行开始时插一行 status='running'，返回 id。
 *
 * 先插再跑（而不是跑完一次性插入）是为了留住崩溃的痕迹：进程被杀时这行会
 * 永久停在 'running'，据此可以发现卡死或 OOM 的运行。一次性插入的话，
 * 崩溃的运行在日志里根本不存在。
 *
 * @returns {Promise<number|null>} 失败返回 null，调用方据此跳过后续更新
 */
export async function startRun(pool, { triggerSource = 'scheduled', sourceDb = null } = {}) {
    try {
        const { rows } = await pool.query(
            `INSERT INTO sync_runs (status, trigger_source, source_db, host)
             VALUES ('running', $1, $2, $3)
             RETURNING id`,
            [triggerSource, sourceDb, os.hostname().slice(0, 128)],
        );
        return rows[0].id;
    } catch (err) {
        console.error(`⚠ 同步日志开始记录失败（不影响同步）: ${err.message}`);
        return null;
    }
}

/**
 * 运行结束时更新那一行。
 *
 * @param {object} opts
 * @param {'success'|'failed'} opts.status
 * @param {object} opts.tables  每张表的 { read, written, deleted }
 * @param {Error}  [opts.error] 失败时的异常
 */
export async function finishRun(pool, id, { status, tables = {}, error = null } = {}) {
    if (id == null) return; // startRun 就失败了，没有行可更新

    const sum = (key) =>
        Object.values(tables).reduce((n, t) => n + (Number(t?.[key]) || 0), 0);

    try {
        await pool.query(
            `UPDATE sync_runs
                SET finished_at   = NOW(),
                    duration_ms   = (EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000)::int,
                    status        = $2,
                    rows_read     = $3,
                    rows_written  = $4,
                    rows_deleted  = $5,
                    tables        = $6::jsonb,
                    error_message = $7
              WHERE id = $1`,
            [
                id,
                status,
                sum('read'),
                sum('written'),
                sum('deleted'),
                JSON.stringify(tables),
                error ? String(error.message ?? error).slice(0, 4000) : null,
            ],
        );
    } catch (err) {
        console.error(`⚠ 同步日志结束记录失败（不影响同步）: ${err.message}`);
    }
}

/**
 * 把上一轮遗留的 'running' 行标记为 failed。
 *
 * 服务启动时调用一次：如果上次进程是被杀/崩溃的，那行会永久停在 'running'，
 * 不清理的话监控里会一直看到"有一次同步在跑"。
 * 只处理**超过 6 小时**还是 running 的，避免误伤同一时刻另一个实例正在跑的运行。
 */
export async function reapStaleRuns(pool) {
    try {
        const { rowCount } = await pool.query(
            `UPDATE sync_runs
                SET status        = 'failed',
                    finished_at   = NOW(),
                    error_message = COALESCE(error_message, '进程未正常结束（启动时清理遗留的 running 记录）')
              WHERE status = 'running'
                AND started_at < NOW() - INTERVAL '6 hours'`,
        );
        if (rowCount > 0) {
            console.log(`  清理了 ${rowCount} 条上次未正常结束的同步记录`);
        }
    } catch (err) {
        console.error(`⚠ 清理遗留同步记录失败（不影响同步）: ${err.message}`);
    }
}

/**
 * 删掉超过保留期的运行记录。
 *
 * 间隔模式每 2 分钟一行 = 每天 720 行、一年约 26 万行，不清理会无限增长。
 * 由 index.js 在启动时和每轮同步后调用；有 started_at 索引，删除本身很便宜。
 * 只删已结束的行 —— 'running' 交给 reapStaleRuns 处理。
 *
 * @param {number} days  保留天数；<= 0 表示不清理
 */
export async function pruneOldRuns(pool, days) {
    if (!(days > 0)) return;
    try {
        const { rowCount } = await pool.query(
            `DELETE FROM sync_runs
              WHERE status <> 'running'
                AND started_at < NOW() - make_interval(days => $1)`,
            [Math.floor(days)],
        );
        if (rowCount > 0) {
            console.log(`  清理了 ${rowCount} 条超过 ${days} 天的同步记录`);
        }
    } catch (err) {
        console.error(`⚠ 清理过期同步记录失败（不影响同步）: ${err.message}`);
    }
}

/** 供健康检查接口用：最近 n 次运行的摘要。 */
export async function recentRuns(pool, n = 5) {
    try {
        const { rows } = await pool.query(
            `SELECT id, started_at, finished_at, duration_ms, status, trigger_source,
                    rows_read, rows_written, rows_deleted, error_message
               FROM sync_runs
              ORDER BY started_at DESC
              LIMIT $1`,
            [n],
        );
        return rows;
    } catch (err) {
        console.error(`⚠ 读取同步记录失败: ${err.message}`);
        return [];
    }
}
