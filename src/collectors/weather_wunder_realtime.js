import { chromium } from 'playwright';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import Redis from 'ioredis';
import pLimit from 'p-limit';

dayjs.extend(utc);
dayjs.extend(timezone);

// 初始化 Redis 客户端
const redis = new Redis({
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: times => Math.min(times * 50, 2000)
});

class WunderRealtimeCollector extends BaseCollector {
  constructor() {
    // 设置 20s 刷新频率
    super('wunder_realtime_monitor', 20 * 1000);
    this.browser = null;
    this.cities = [];
    this.lastRestartTime = Date.now();
    this.RESTART_INTERVAL = 10 * 60 * 1000; // 10分钟重启阈值
    this.isBrowserInitializing = false;    // 防止并发初始化冲突
  }

  /**
   * 浏览器常驻与自动重启机制（线程安全）
   */
  async ensureBrowser() {
    if (this.isBrowserInitializing) return;

    const now = Date.now();
    const needsRestart = !this.browser || (now - this.lastRestartTime > this.RESTART_INTERVAL);

    if (needsRestart) {
      this.isBrowserInitializing = true;
      try {
        if (this.browser) {
          logger.info('♻️ Periodic browser restart to maintain performance...');
          await this.browser.close().catch(() => {});
        }
        this.browser = await chromium.launch({
          headless: true,
          args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-zygote'
          ]
        });
        this.lastRestartTime = now;
        logger.info('🚀 Browser (re)started and ready.');
      } catch (err) {
        logger.error(`❌ Browser launch failed: ${err.message}`);
      } finally {
        this.isBrowserInitializing = false;
      }
    }
  }

  async loadCitiesConfig() {
    try {
      const rawData = await redis.hgetall('poly:config:cities');
      const loadedCities = Object.values(rawData).map(jsonStr => {
        try { return JSON.parse(jsonStr); } catch (e) { return null; }
      }).filter(item => item !== null);

      if (loadedCities.length > 0) {
        this.cities = loadedCities;
      } else {
        logger.warn('⚠️ Redis config is empty (poly:config:cities).');
      }
    } catch (e) {
      logger.error(`❌ Failed to load cities config: ${e.message}`);
    }
  }

  convertTemperature(val, fromUnit, toUnit) {
    if (fromUnit === toUnit) return val;
    if (fromUnit === 'F' && toUnit === 'C') return Math.round((val - 32) / 1.8);
    if (fromUnit === 'C' && toUnit === 'F') return Math.round(val * 1.8 + 32);
    return val;
  }

  async getProxy() {
    try {
      const proxyStr = await redis.srandmember('poly:proxylist');
      if (!proxyStr) return null;
      const parts = proxyStr.split(':');
      if (parts.length < 4) return null;
      return {
        server: `http://${parts[0]}:${parts[1]}`,
        username: parts[2],
        password: parts[3]
      };
    } catch (e) { return null; }
  }

  /**
   * 单个城市抓取逻辑
   */
  async scrapeRealtime(cityConfig) {
    const localNow = dayjs().tz(cityConfig.tz);
    const stdDate = localNow.format('YYYY-MM-DD');
    const baseUrl = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}`;
    const url = `${baseUrl}?_t=${Date.now()}`;
    
    let context = null;
    let page = null;
    const result = { meta: cityConfig, date_std: stdDate, current: null, station_time: null, success: false };

    try {
      const proxyConfig = await this.getProxy();

      context = await this.browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 800, height: 600 },
        proxy: proxyConfig || undefined
      });

      await context.setExtraHTTPHeaders({
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });

      page = await context.newPage();
      
      // 优化：禁止加载无关资源
      await page.route('**/*.{png,jpg,jpeg,gif,svg,mp4,woff,woff2,css}', route => route.abort());

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.waitForSelector('.current-temp .wu-value', { timeout: 15000 });

      const pageData = await page.evaluate(() => {
        const valueText = document.querySelector('.current-temp .wu-value')?.innerText || "";
        const unitText = document.querySelector('.current-temp .wu-label')?.innerText || "";
        const timeText = document.querySelector('.city-conditions .timestamp strong')?.innerText || "";
        return {
          rawValue: parseInt(valueText.trim()),
          pageUnit: unitText.includes('C') ? 'C' : 'F',
          stationTime: timeText.trim()
        };
      });

      if (!isNaN(pageData.rawValue)) {
        const finalValue = this.convertTemperature(pageData.rawValue, pageData.pageUnit, cityConfig.unit);
        result.current = finalValue;
        result.station_time = pageData.stationTime;
        result.success = true;
      }
    } catch (error) {
      logger.error(`❌ [${cityConfig.name}] Scrape Error: ${error.message}`);
    } finally {
      if (page) await page.close().catch(() => {});
      if (context) await context.close().catch(() => {});
    }
    return result;
  }

  async syncToRedis(data) {
    if (!data.success) return;
    const { meta, date_std, current } = data;
    const key = `poly:latest:weather:${meta.station}`;

    try {
      const cache = await redis.hmget(key, 'rt_high', 'target_date');
      const cachedHigh = cache[0];
      const cachedDate = cache[1];

      let rtRollingHigh = current;

      if (cachedHigh && cachedDate === date_std) {
        const oldHigh = parseInt(cachedHigh);
        if (!isNaN(oldHigh) && oldHigh > current) {
          rtRollingHigh = oldHigh;
        }
      }

      // 批量写入最新状态
      await redis.hmset(key, {
        city_name: meta.name,
        target_date: date_std,
        unit: meta.unit,
        rt_current: current,
        rt_high: rtRollingHigh,
        rt_ts: Date.now(),
        rt_station_time: data.station_time || '',
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
      });
      
      // 写入时间序列
      const vizKey = `poly:viz:series:${meta.station}`;
      const ts = Date.now();
      const member = JSON.stringify({ t: current, s: 'rt', ts: ts });
      
      const pipeline = redis.pipeline();
      pipeline.zadd(vizKey, ts, member);
      pipeline.zremrangebyscore(vizKey, '-inf', ts - 86400000); // 仅保留24小时
      pipeline.publish(`poly:feed:weather:${meta.station}`, JSON.stringify({ station: meta.station, type: 'rt', ts }));
      await pipeline.exec();

      logger.info(`✨ [${meta.name}] ${current}°${meta.unit} (High: ${rtRollingHigh}°${meta.unit})`);
    } catch (e) {
      logger.error(`❌ [${meta.name}] Redis Sync Error: ${e.message}`);
    }
  }

  /**
   * 调度核心：并发执行
   */
  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) return { timestamp: Date.now() };

    await this.ensureBrowser();
    
    // 限制最大并发数为 3，避免代理被封或 CPU 负载过高
    const limit = pLimit(3);
    
    const tasks = this.cities.map(city => {
      return limit(async () => {
        const data = await this.scrapeRealtime(city);
        await this.syncToRedis(data);
        // 抓取后随机休眠 1-2 秒，模拟人工并降低目标站压力
        await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 1000));
      });
    });

    await Promise.allSettled(tasks);
    
    return { timestamp: Date.now() };
  }
}

export default WunderRealtimeCollector;