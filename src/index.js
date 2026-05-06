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


// 在这里注册所有启用的采集器
const collectors = [
  // new CDCFluCollector(),      // 独立运行流感
  // new CDCMeaslesCollector(),  // 独立运行麻疹
  // new WeatherMonitorCollector(), // NOAA + OpenMeteo (高频监控)
  // new WunderRealtimeCollector(),
  // new WunderHistoryCollector(),
  // new WunderAxiosCollector(),
  // new WunderDualCollector(),
  new WeatherFlowPlusCollector(),
  new WeatherSettlementCollector()  // historical METAR 实测数据，用于结算对账
];

async function main() {
  logger.info('Starting Poly Feed Receiver...');

  // 启动所有采集器
  for (const collector of collectors) {
    // 不等待 start 返回（因为它是死循环），让它们并发运行
    collector.start().catch(err => {
      logger.error(`Failed to start collector ${collector.sourceId}`, { error: err.message });
    });
  }

  // 优雅退出处理
  const shutdown = async (signal) => {
    logger.info(`Received ${signal}, shutting down...`);
    
    // 1. 停止所有采集循环
    collectors.forEach(c => c.stop());
    
    // 2. 等待 pending 的请求完成 (简单等待几秒)
    // 更好的做法是 BaseCollector 内部维护一个 promise
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // 3. 断开 Redis 连接
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

main().catch(err => {
  logger.error('Fatal startup error', { error: err.message });
  process.exit(1);
});
