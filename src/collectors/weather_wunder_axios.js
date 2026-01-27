import axios from 'axios';
import * as cheerio from 'cheerio';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import Redis from 'ioredis';
import pLimit from 'p-limit';

dayjs.extend(utc);
dayjs.extend(timezone);

const redis = new Redis({
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: times => Math.min(times * 50, 2000)
});

class WunderAxiosCollector extends BaseCollector {
  constructor() {
    super('wunder_axios_monitor', 20 * 1000);
    this.cities = [];
  }

  async loadCitiesConfig() {
    try {
      const rawData = await redis.hgetall('poly:config:cities');
      this.cities = Object.values(rawData).map(s => JSON.parse(s)).filter(Boolean);
    } catch (e) {
      logger.error(`❌ Load Config Error: ${e.message}`);
    }
  }

  // 温度转换工具
  convertTemperature(val, fromUnit, toUnit) {
    if (!fromUnit || !toUnit || fromUnit === toUnit) return val;
    if (fromUnit === 'F' && toUnit === 'C') return Math.round((val - 32) / 1.8);
    if (fromUnit === 'C' && toUnit === 'F') return Math.round(val * 1.8 + 32);
    return val;
  }

  async getAxiosProxy() {
    try {
      const proxyStr = await redis.srandmember('poly:proxylist');
      if (!proxyStr) return null;
      const [host, port, user, pass] = proxyStr.split(':');
      return {
        protocol: 'http',
        host: host,
        port: parseInt(port),
        auth: user ? { username: user, password: pass } : undefined
      };
    } catch (e) { return null; }
  }

  async scrapeByAxios(cityConfig) {
    const url = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}`;
    const result = { meta: cityConfig, success: false, current: null, station_time: null, temp_max_24h: null };

    try {
      const proxy = await this.getAxiosProxy();
      const response = await axios.get(url, {
        proxy: proxy || false,
        timeout: 12000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache'
        }
      });

      const $ = cheerio.load(response.data);
      const scriptContent = $('#app-root-state').html();
      if (!scriptContent) throw new Error('Missing #app-root-state');

      const rawData = JSON.parse(scriptContent);

      // 深度特征搜索：寻找包含 temperature 的数据块
      let obsData = null;
      for (const key in rawData) {
        if (rawData[key]?.b && typeof rawData[key].b.temperature === 'number') {
          obsData = rawData[key].b;
          break;
        }
      }

      if (obsData) {
        const rawVal = obsData.temperature;   
        let pageUnit = 'F'; 
        
        // 2. 转换温度到目标单位
        result.current = this.convertTemperature(rawVal, pageUnit, cityConfig.unit);
        
        result.station_time = obsData.validTimeLocal || obsData.obsTimeLocal;
        result.success = true;
      }
    } catch (error) {
      logger.error(`❌ [${cityConfig.name}] Axios Error: ${error.message}`);
    }
    return result;
  }

  async syncToRedis(data) {
    if (!data.success) return;
    const { meta, current } = data;
    const date_std = dayjs().tz(meta.tz).format('YYYY-MM-DD');
    const key = `poly:latest:weather:${meta.station}`;

    try {
      const cached = await redis.hmget(key, 'rt_high', 'target_date');
      let rtHigh = current;
      
      // 如果 24h 最高温失效，使用 Redis 滚动对比
      if (cached[1] === date_std && cached[0]) {
        rtHigh = Math.max(parseFloat(cached[0]), rtHigh);
      }

      const pipeline = redis.pipeline();
      pipeline.hmset(key, {
        city_name: meta.name,
        target_date: date_std,
        unit: meta.unit,
        rt_current: current,
        rt_high: rtHigh,
        rt_ts: Date.now(),
        rt_station_time: data.station_time || '',
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
      });

      const vizKey = `poly:viz:series:${meta.station}`;
      const ts = Date.now();
      pipeline.zadd(vizKey, ts, JSON.stringify({ t: current, s: 'rt', ts }));
      pipeline.zremrangebyscore(vizKey, '-inf', ts - 86400000);
      pipeline.publish(`poly:feed:weather:${meta.station}`, JSON.stringify({ station: meta.station, ts }));
      
      await pipeline.exec();
      logger.info(`✨ [${meta.name}] ${current}°${meta.unit} | Max: ${rtHigh}°${meta.unit}`);
    } catch (e) {
      logger.error(`❌ Redis Sync Error: ${e.message}`);
    }
  }

  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) return;

    const limit = pLimit(8);
    const tasks = this.cities.map(city => limit(async () => {
      const data = await this.scrapeByAxios(city);
      await this.syncToRedis(data);
    }));

    await Promise.allSettled(tasks);
    return { timestamp: Date.now() };
  }
}

export default WunderAxiosCollector;