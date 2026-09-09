import dotenv from 'dotenv';
import http from 'http';
import { syncAllTables, pgPool } from './sync-simple.js';
import { reapStaleRuns, recentRuns } from './syncLog.js';

dotenv.config();

const PORT = process.env.PORT || 3000;

// ─── 调度：每天布里斯班时间 00:00 ─────────────────────────────────────────────
//
// 2026-09-09 从"每 2 分钟一次"改成"每天一次"。
//
// ⚠️ 由此带来的后果必须清楚：老站 / adminpage 改的数据，Pro 侧最长要 24 小时
//    才能看到。双写期靠"2 分钟同步自愈"的那些设计（docs/migration/04、06 的
//    不变式 I2）现在自愈窗口变成一天 —— sync_source='pro' 的保护因此更关键。
//    Pro 正式上线前需要重新评估这个间隔。
//
// Australia/Brisbane 全年 UTC+10（昆士兰不实行夏令时），所以不存在"某天 23 小时
// 或 25 小时"的问题。这里仍按 IANA 时区名计算而不是写死 +10，万一将来昆州改了
// 政策也不用动代码。
const SYNC_TIMEZONE = process.env.SYNC_TIMEZONE || 'Australia/Brisbane';
const SYNC_AT_HOUR = Number(process.env.SYNC_AT_HOUR ?? 0); // 0 = 午夜
// 启动时是否立刻补一次（默认是：部署完不用等到半夜才知道能不能跑通）
const SYNC_ON_STARTUP = process.env.SYNC_ON_STARTUP !== 'false';

/** 返回距离目标时区下一个 SYNC_AT_HOUR:00:00 的毫秒数。 */
function msUntilNextRun(now = new Date()) {
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
          timezone: SYNC_TIMEZONE,
          atHour: SYNC_AT_HOUR,
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
  const pad = (n) => String(n).padStart(2, '0');
  console.log('='.repeat(60));
  console.log('SQL Server 到 PostgreSQL 定时同步服务');
  console.log('='.repeat(60));
  console.log(`同步时刻: 每天 ${pad(SYNC_AT_HOUR)}:00 (${SYNC_TIMEZONE})`);
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

  // 用 setTimeout 递归而不是 setInterval：每次跑完重新按时区算下一次，
  // 这样不会因为累计漂移或夏令时（本时区没有，但别的时区会）而偏离目标时刻。
  const scheduleNext = () => {
    const wait = msUntilNextRun();
    const at = new Date(Date.now() + wait);
    console.log(
      `\n✓ 下次同步: ${fmtInTz(at)} (${SYNC_TIMEZONE})，` +
      `${(wait / 3600000).toFixed(1)} 小时后`,
    );
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
