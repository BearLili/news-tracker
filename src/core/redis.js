import Redis from 'ioredis';
import config from '../config/index.js';
import logger from '../utils/logger.js';

// 创建 Redis 单例
const redis = new Redis(config.redisUrl, {
  // ioredis 默认有重连机制，这里微调重试策略
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
  // 高频场景下，如果 Redis 挂了，我们希望快速失败而不是阻塞内存
  maxRetriesPerRequest: 1 
});

redis.on('connect', () => {
  logger.info('Redis connected successfully');
});

redis.on('error', (err) => {
  // 捕获全局 Redis 错误，防止抛出未捕获异常导致 Node 进程退出
  logger.error('Redis Client Error', { error: err.message });
});

export default redis;
