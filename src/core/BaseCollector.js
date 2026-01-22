import redis from './redis.js';
import logger from '../utils/logger.js';

class BaseCollector {
  /**
   * @param {string} sourceId - 数据源唯一标识，如 'polymarket_btc'
   * @param {number} intervalMs - 采集间隔(毫秒)
   */
  constructor(sourceId, intervalMs = 1000) {
    if (!sourceId) throw new Error('sourceId is required');
    this.sourceId = sourceId;
    this.intervalMs = intervalMs;
    this.isRunning = false;
  }

  /**
   * 抽象方法：具体采集逻辑
   * @returns {Promise<any>} 返回采集到的原始数据
   */
  async fetch() {
    throw new Error('Method "fetch()" must be implemented.');
  }

  /**
   * 核心方法：统一封装与推送
   * @param {any} rawData 
   */
  async save(rawData) {
    if (rawData === null || rawData === undefined) return;

    try {
        const payload = {
            source: this.sourceId,
            ts: Date.now(),
            data: rawData
        };
        const message = JSON.stringify(payload);
        
        // 1. 广播通道 (给机器人用)
        const channel = `poly:feed:${this.sourceId}`;
        await redis.publish(channel, message);

        // 2. [新增] 缓存最新值 (给你看，或者给掉线重连的程序看)
        // 增加一个 kv 键，比如 poly:latest:cdc_combined
        const key = `poly:latest:${this.sourceId}`;
        await redis.set(key, message); 
        // 可选：设置过期时间，比如 24小时，防止僵尸数据
        // await redis.expire(key, 86400);

        logger.debug(`Saved ${this.sourceId} (Pub + Set)`);
    } catch (error) {
        logger.error(`Failed to save data for ${this.sourceId}`, { error: error.message });
    }
}

  /**
   * 启动循环
   */
  async start() {
    if (this.isRunning) return;
    this.isRunning = true;
    logger.info(`Collector started: ${this.sourceId} (interval: ${this.intervalMs}ms)`);
    this.runLoop();
  }

  /**
   * 停止循环
   */
  stop() {
    this.isRunning = false;
    logger.info(`Collector stopped: ${this.sourceId}`);
  }

  /**
   * 内部运行循环
   * 使用 while + setTimeout 替代 setInterval，避免请求堆积
   */
  async runLoop() {
    let loopCount = 0; // 计数器

    while (this.isRunning) {
      const start = Date.now();
      loopCount++;

      try {
        // 1. 执行采集
        const data = await this.fetch();
        
        // 2. 执行存储 (如果有数据)
        if (data) {
           await this.save(data);
           loopCount = 0; // 如果有新数据，重置计数器
        } else {
           // --- 新增：心跳机制 ---
           // 每 N 次空轮询，打印一次心跳。
           // 假设 interval 是 60秒，这里设置 10，就是每 10分钟打印一次
           if (loopCount % 10 === 0) {
             logger.info(`💓 [Heartbeat] ${this.sourceId} is running. No updates found.`);
           }
        }

      } catch (error) {
        logger.error(`Error in collector ${this.sourceId}`, { error: error.message, stack: error.stack });
      }

      // 计算休眠时间
      const elapsed = Date.now() - start;
      const sleepTime = Math.max(0, this.intervalMs - elapsed);
      
      if (this.isRunning) {
        await new Promise(resolve => setTimeout(resolve, sleepTime));
      }
    }
  }
}

export default BaseCollector;
