import dotenv from 'dotenv';
import http from 'http';
import { syncAllTables } from './sync-simple.js';

dotenv.config();

const PORT = process.env.PORT || 3000;
const SYNC_INTERVAL_MS = 2 * 60 * 1000; // 2分钟

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
async function performSync() {
  if (syncStatus.isRunning) {
    console.log('⚠️  同步任务正在运行中，跳过本次执行');
    return;
  }

  syncStatus.isRunning = true;
  syncStatus.lastSyncTime = new Date();
  
  try {
    console.log(`\n[${new Date().toISOString()}] 开始执行同步任务...`);
    // 首次同步时重建数据库结构，后续同步不重建
    const isFirstSync = syncStatus.syncCount === 0;
    await syncAllTables(false, false, isFirstSync);
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
      const healthStatus = {
        status: 'ok',
        service: 'sql-to-pg-sync',
        timestamp: new Date().toISOString(),
        sync: {
          isRunning: syncStatus.isRunning,
          lastSyncTime: syncStatus.lastSyncTime,
          syncCount: syncStatus.syncCount,
          lastError: syncStatus.lastSyncError,
        },
      };

      res.writeHead(200);
      res.end(JSON.stringify(healthStatus, null, 2));
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
function startScheduledSync() {
  console.log('='.repeat(60));
  console.log('SQL Server 到 PostgreSQL 定时同步服务');
  console.log('='.repeat(60));
  console.log(`同步间隔: ${SYNC_INTERVAL_MS / 1000 / 60} 分钟`);
  console.log(`健康检查端口: ${PORT}`);
  console.log('='.repeat(60));

  // 立即执行一次同步
  console.log('\n执行首次同步...');
  performSync();

  // 设置定时器，每2分钟执行一次
  setInterval(() => {
    performSync();
  }, SYNC_INTERVAL_MS);

  console.log(`\n✓ 定时同步任务已启动，每 ${SYNC_INTERVAL_MS / 1000 / 60} 分钟执行一次`);
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
  startScheduledSync();
  setupGracefulShutdown(server);
} catch (error) {
  console.error('✗ 服务启动失败:', error);
  process.exit(1);
}
