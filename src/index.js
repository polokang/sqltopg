import dotenv from 'dotenv';
import http from 'http';
import { syncAllTables, pgPool } from './sync-simple.js';
import { reapStaleRuns, recentRuns } from './syncLog.js';

dotenv.config();

const PORT = process.env.PORT || 3000;

// ─── 调度 ─────────────────────────────────────────────────────────────────────
//
// 两种模式，由 SYNC_INTERVAL_MINUTES 决定：
//
//   SYNC_INTERVAL_MINUTES = 0   → **每天定时**：在 SYNC_TIMEZONE 的
//                                 SYNC_AT_HOUR:00 跑一次
//   SYNC_INTERVAL_MINUTES > 0   → **按间隔**：每 N 分钟跑一次，
//                                 SYNC_AT_HOUR / SYNC_TIMEZONE 不再参与调度
//
// ⚠️ 间隔模式有 5 分钟下限。低于下限会被提升到 5 并告警 —— 不是拍脑袋定的：
//    2026-09-09 实测（本机 → Azure PG）单轮约 2 秒（1.5 秒读源库 + 批量写），
//    5 分钟留了两个数量级的余量。真要更密，先把单轮耗时测出来再说。
//    另外 performSync() 有 isRunning 保护，上一轮没跑完不会重入。
//
// ⚠️ 间隔模式改变了双写的自愈窗口。docs/migration/04、06 的不变式 I2
//    （"mirror 失败只记日志，靠同步自愈"）里，自愈延迟 = 本间隔。
//    每天一次 = 最长 24 小时；设成 5~15 分钟就回到接近原先的水平。
//    见台账 R-213。
//
// Australia/Brisbane 全年 UTC+10（昆士兰不实行夏令时），所以每天定时模式不存在
// "某天 23 小时或 25 小时"的问题。这里仍按 IANA 时区名计算而不是写死 +10，
// 万一将来昆州改了政策也不用动代码。
const SYNC_TIMEZONE = process.env.SYNC_TIMEZONE || 'Australia/Brisbane';
const SYNC_AT_HOUR = Number(process.env.SYNC_AT_HOUR ?? 0); // 0 = 午夜
// 启动时是否立刻补一次（默认是：部署完不用等到下一个周期才知道能不能跑通）
const SYNC_ON_STARTUP = process.env.SYNC_ON_STARTUP !== 'false';

/** 间隔模式的下限（分钟）。 */
const MIN_INTERVAL_MINUTES = 5;

/**
 * 解析 SYNC_INTERVAL_MINUTES，决定调度模式。
 * 非法值一律退回"每天定时"并告警 —— 静默按某个默认值跑，比报错更难查。
 */
function resolveSchedule() {
  const raw = String(process.env.SYNC_INTERVAL_MINUTES ?? '').trim();
  if (raw === '') return { mode: 'daily' };

  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    console.warn(
      `⚠ SYNC_INTERVAL_MINUTES="${raw}" 不是有效的非负数字，` +
      `按 0 处理（改为每天 ${SYNC_AT_HOUR}:00 ${SYNC_TIMEZONE} 定时同步）。`,
    );
    return { mode: 'daily' };
  }
  if (n === 0) return { mode: 'daily' };

  if (n < MIN_INTERVAL_MINUTES) {
    console.warn(
      `⚠ SYNC_INTERVAL_MINUTES=${n} 低于下限 ${MIN_INTERVAL_MINUTES} 分钟，已提升为 ` +
      `${MIN_INTERVAL_MINUTES} 分钟。要跑得更密，先实测单轮耗时（看 sync_runs.duration_ms）。`,
    );
    return { mode: 'interval', minutes: MIN_INTERVAL_MINUTES };
  }
  return { mode: 'interval', minutes: n };
}

const SCHEDULE = resolveSchedule();

/** 距离下一次同步的毫秒数。两种模式共用这一个入口。 */
function msUntilNextRun(now = new Date()) {
  if (SCHEDULE.mode === 'interval') {
    return SCHEDULE.minutes * 60 * 1000;
  }

  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: SYNC_TIMEZONE,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
    .formatToParts(now)
    .reduce((acc, p) => (acc[p.type] = p.value, acc), {});

  // hour12:false 在部分 ICU 版本下会把午夜给成 "24"，取模兜住
  const h = Number(parts.hour) % 24;
  const m = Number(parts.minute);
  const s = Number(parts.second);

  const nowMs    = ((h * 60 + m) * 60 + s) * 1000 + now.getMilliseconds();
  const targetMs = SYNC_AT_HOUR * 3600 * 1000;

  let wait = targetMs - nowMs;
  if (wait <= 0) wait += 24 * 3600 * 1000; // 今天这个点已过，等明天
  return wait;
}

/** 一句话描述当前调度，日志与 /health 共用。 */
function describeSchedule() {
  return SCHEDULE.mode === 'interval'
    ? `每 ${SCHEDULE.minutes} 分钟一次`
    : `每天 ${String(SYNC_AT_HOUR).padStart(2, '0')}:00 (${SYNC_TIMEZONE})`;
}

/** 用目标时区把时间格式化出来，日志里好读。 */
function fmtInTz(d) {
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: SYNC_TIMEZONE,
    dateStyle: 'short',
    timeStyle: 'medium',
  }).format(d);
}

// 同步状态
let syncStatus = {
  isRunning: false,
  lastSyncTime: null,
  lastSyncError: null,
  syncCount: 0,
};

/**
 * 执行同步任务
 */
async function performSync(triggerSource = 'scheduled') {
  if (syncStatus.isRunning) {
    console.log('⚠️  同步任务正在运行中，跳过本次执行');
    return;
  }

  syncStatus.isRunning = true;
  syncStatus.lastSyncTime = new Date();
  
  try {
    console.log(`\n[${new Date().toISOString()}] 开始执行同步任务...`);
    // 常驻服务**永远不重建 schema**。建表只由 src/initDatabase.js 负责，而它是
    // "删库重建"，绝不能挂在定时任务上。
    // （这里原先传过一个 isFirstSync 并注释成"首次同步时重建数据库结构"，实际被
    //   syncAllTables 忽略 —— 留着这种注释迟早有人照着实现，已删。）
    await syncAllTables(false, { triggerSource });
    syncStatus.syncCount++;
    syncStatus.lastSyncError = null;
    console.log(`[${new Date().toISOString()}] ✓ 同步任务完成`);
  } catch (error) {
    syncStatus.lastSyncError = {
      message: error.message,
      time: new Date().toISOString(),
    };
    console.error(`[${new Date().toISOString()}] ✗ 同步任务失败:`, error.message);
  } finally {
    syncStatus.isRunning = false;
  }
}

/**
 * 创建 HTTP 服务器用于健康检查
 */
function createHealthCheckServer() {
  const server = http.createServer((req, res) => {
    // 设置 CORS 头
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json; charset=utf-8');

    if (req.url === '/health' || req.url === '/') {
      // 最近几次运行从 sync_runs 表读，比内存里的计数器可靠 —— 进程重启后
      // 内存计数器归零，而表里的历史还在。
      recentRuns(pgPool, 5).then((runs) => {
      const healthStatus = {
        status: 'ok',
        service: 'sql-to-pg-sync',
        timestamp: new Date().toISOString(),
        schedule: {
          mode: SCHEDULE.mode,                       // 'interval' | 'daily'
          intervalMinutes: SCHEDULE.minutes ?? null, // 仅 interval 模式有值
          timezone: SYNC_TIMEZONE,                   // 仅 daily 模式参与调度
          atHour: SYNC_AT_HOUR,                      // 仅 daily 模式参与调度
          description: describeSchedule(),
          nextRunAt: new Date(Date.now() + msUntilNextRun()).toISOString(),
        },
        sync: {
          isRunning: syncStatus.isRunning,
          lastSyncTime: syncStatus.lastSyncTime,
          syncCount: syncStatus.syncCount,
          lastError: syncStatus.lastSyncError,
        },
        recentRuns: runs,
      };

      res.writeHead(200);
      res.end(JSON.stringify(healthStatus, null, 2));
      });
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: 'Not Found' }));
    }
  });

  server.listen(PORT, () => {
    console.log(`✓ 健康检查服务已启动，监听端口 ${PORT}`);
    console.log(`  访问 http://localhost:${PORT}/health 查看状态`);
  });

  return server;
}

/**
 * 启动定时同步任务
 */
async function startScheduledSync() {
  console.log('='.repeat(60));
  console.log('SQL Server 到 PostgreSQL 定时同步服务');
  console.log('='.repeat(60));
  console.log(`同步调度: ${describeSchedule()}`);
  console.log(`当前时间: ${fmtInTz(new Date())} (${SYNC_TIMEZONE})`);
  console.log(`健康检查端口: ${PORT}`);
  console.log('='.repeat(60));

  // 清理上一轮进程被杀 / 崩溃后遗留的 'running' 记录，否则监控里会一直
  // 看到"有一次同步在跑"
  await reapStaleRuns(pgPool);

  if (SYNC_ON_STARTUP) {
    console.log('\n执行启动时同步...');
    await performSync('startup');
  } else {
    console.log('\nSYNC_ON_STARTUP=false，跳过启动时同步');
  }

  // 用 setTimeout 递归而不是 setInterval，两种模式都受益：
  //   每天定时 —— 每轮跑完重新按时区算下一次，不会累计漂移，夏令时也正确
  //   按间隔   —— 间隔从"上一轮结束"起算，保证两轮之间真有这么长的空档，
  //               不会出现上一轮还没跑完下一轮就排上的情况
  const scheduleNext = () => {
    const wait = msUntilNextRun();
    const at = new Date(Date.now() + wait);
    const rel = wait >= 3600000
      ? `${(wait / 3600000).toFixed(1)} 小时后`
      : `${Math.round(wait / 60000)} 分钟后`;
    console.log(`\n✓ 下次同步: ${fmtInTz(at)} (${SYNC_TIMEZONE})，${rel}`);
    setTimeout(async () => {
      await performSync('scheduled');
      scheduleNext();
    }, wait);
  };
  scheduleNext();
}

/**
 * 优雅关闭处理
 */
function setupGracefulShutdown(server) {
  const shutdown = async (signal) => {
    console.log(`\n收到 ${signal} 信号，正在关闭服务...`);
    
    // 停止接受新请求
    server.close(() => {
      console.log('✓ HTTP 服务器已关闭');
    });

    // 等待当前同步任务完成
    if (syncStatus.isRunning) {
      console.log('等待当前同步任务完成...');
      while (syncStatus.isRunning) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    console.log('✓ 服务已安全关闭');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

// 启动服务
try {
  const server = createHealthCheckServer();
  await startScheduledSync();
  setupGracefulShutdown(server);
} catch (error) {
  console.error('✗ 服务启动失败:', error);
  process.exit(1);
}
