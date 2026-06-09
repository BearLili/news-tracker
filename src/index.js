import logger from './utils/logger.js';
import redis from './core/redis.js';
import WeatherMonitorCollector from './collectors/weather_monitor.js';
import CDCFluCollector from './collectors/cdc_flu_collector.js';
import CDCMeaslesCollector from './collectors/cdc_measles_collector.js';
import WunderRealtimeCollector from './collectors/weather_wunder_realtime.js';
import WunderHistoryCollector from './collectors/weather_wunder_history.js';
import WunderAxiosCollector from './collectors/weather_wunder_axios.js';
import WunderDualCollector from './collectors/weather_wunder_dual.js';
import WeatherForecastCollector from './collectors/weather_flow.js';
import WeatherFlowPlusCollector from './collectors/weather_flow_plus.js';
import WeatherSettlementCollector from './collectors/weather_settlement.js';
import WeatherObservationsCollector from './collectors/weather_observations.js';
import NPMPricingCollector from './collectors/npm_pricing.js';
import { startWebServer, stopWebServer } from './web/server.js';


// 在这里注册所有启用的采集器
const collectors = [
  // new CDCFluCollector(),      // 独立运行流感
  // new CDCMeaslesCollector(),  // 独立运行麻疹
  // new WeatherMonitorCollector(), // NOAA + OpenMeteo (高频监控)
  // new WunderRealtimeCollector(),
  // new WunderHistoryCollector(),
  // new WunderAxiosCollector(),
  // new WunderDualCollector(),
  // new WeatherSettlementCollector(),  // 已被 WeatherObservationsCollector 取代（保留以便回滚）
  new WeatherFlowPlusCollector(),
  new WeatherObservationsCollector(), // 结算（TWC 历史 METAR）+ 预记录（aviationweather METAR）二合一
  new NPMPricingCollector()           // Nasdaq Private Market 私募公司估值
];

async function main() {
  logger.info('Starting Poly Feed Receiver...');

  // 启动前校验：如果有任何城市启用了 wethr，但 WETHR_API_KEY 没配，硬失败退出
  // 不静默 fallback 是为了避免"以为 wethr 在跑实际没在跑"的误解
  await validateWethrConfig();

  // 启动所有采集器
  for (const collector of collectors) {
    // 不等待 start 返回（因为它是死循环），让它们并发运行
    collector.start().catch(err => {
      logger.error(`Failed to start collector ${collector.sourceId}`, { error: err.message });
    });
  }

  // 启动 Web 可视化（同进程，仅本机访问）
  // 设置 WEB_DISABLED=true 可禁用（例如 headless 部署只跑采集器）
  if (process.env.WEB_DISABLED !== 'true') {
    try { await startWebServer(); }
    catch (e) { logger.error(`Failed to start web server: ${e.message}`); }
  }

  // 优雅退出处理
  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, shutting down...`);

    // 1. 停止所有采集循环，并 await 那些异步释放外部资源的（比如 wethr SSE slot）
    //    BaseCollector.stop() 是 sync 但子类可以 override 成 async（WeatherObservationsCollector 就是）
    //    用 Promise.resolve 包裹兼容两种返回
    await Promise.all(collectors.map(c => {
      try { return Promise.resolve(c.stop()); }
      catch (e) { logger.warn(`collector stop error: ${e.message}`); return Promise.resolve(); }
    }));

    // 2. 关闭 Web 服务
    await stopWebServer().catch(() => {});

    // 3. 给 pending HTTP 请求一点时间收尾
    await new Promise(resolve => setTimeout(resolve, 1000));

    // 4. 断开 Redis 连接
    await redis.quit();

    logger.info('Cleanup finished. Bye.');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  
  // 处理未捕获异常，避免僵尸进程
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught Exception', { error: err.message, stack: err.stack });
    // 对于高可用服务，通常建议这里退出并由守护进程(PM2/Docker)重启
    // shutdown('uncaughtException'); 
  });
}

/**
 * 启动校验：扫一遍 poly:config:cities，如果有任何 city 启用 wethrEnabled 但
 * 进程 env 缺 WETHR_API_KEY，就硬失败退出（不静默 fallback）。
 * 这样误漏配 key 会立刻被 pm2/Docker 重启看到，而不是悄悄不订阅。
 */
async function validateWethrConfig() {
  try {
    const raw = await redis.hgetall('poly:config:cities');
    const enabled = Object.values(raw).map(s => {
      try { return JSON.parse(s); } catch { return null; }
    }).filter(c => {
      if (!c) return false;
      const v = c.wethrEnabled;
      return v === true || v === 'true' || v === '1' || v === 1;
    });
    if (enabled.length === 0) return;
    if (enabled.length > 5) {
      logger.error(`Fatal: ${enabled.length} cities have wethrEnabled=true but wethr Professional tier 限 5 站，请减少配置后重启`);
      process.exit(1);
    }
    if (!process.env.WETHR_API_KEY) {
      logger.error(`Fatal: ${enabled.length} cities have wethrEnabled=true (${enabled.map(c => c.station).join(',')}) but WETHR_API_KEY env var missing`);
      process.exit(1);
    }
    logger.info(`✅ wethr config validated: ${enabled.length} stations (${enabled.map(c => c.station).join(',')})`);
  } catch (e) {
    logger.error(`Fatal: wethr config validation failed: ${e.message}`);
    process.exit(1);
  }
}

main().catch(err => {
  logger.error('Fatal startup error', { error: err.message });
  process.exit(1);
});
