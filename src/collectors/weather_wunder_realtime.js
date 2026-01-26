import { chromium } from 'playwright';
import BaseCollector from '../core/BaseCollector.js';
import logger from '../utils/logger.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';
import Redis from 'ioredis';

dayjs.extend(utc);
dayjs.extend(timezone);

const redis = new Redis({
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: times => Math.min(times * 50, 2000)
});

class WunderRealtimeCollector extends BaseCollector {
  constructor() {
    // 1. 刷新频率改为 20s
    super('wunder_realtime_monitor', 20 * 1000);
    this.browser = null;
    this.cities = [];
    this.lastRestartTime = Date.now();
    this.RESTART_INTERVAL = 10 * 60 * 1000; // 10分钟重启阈值
  }

  /**
   * 核心优化：浏览器常驻，带自动重启机制
   */
  async ensureBrowser() {
    const now = Date.now();
    // 如果浏览器不存在，或者超过10分钟，执行重启
    if (!this.browser || (now - this.lastRestartTime > this.RESTART_INTERVAL)) {
      if (this.browser) {
        logger.info('♻️ Periodic browser restart to maintain performance...');
        await this.browser.close().catch(() => {});
      }
      this.browser = await chromium.launch({ 
        headless: true,
        args: [
          '--no-sandbox', 
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage', // 防止大内存占用挂死
          '--disable-gpu',
          '--no-zygote'
        ]
      });
      this.lastRestartTime = now;
      logger.info('🚀 Browser (re)started and ready.');
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

  async scrapeRealtime(cityConfig) {
    const localNow = dayjs().tz(cityConfig.tz);
    const stdDate = localNow.format('YYYY-MM-DD'); 
    
    // 增加随机数防止 CDN 缓存
    const baseUrl = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}`;
    const url = `${baseUrl}?_t=${Date.now()}`;
    
    let context = null;
    let page = null;
    const result = { meta: cityConfig, date_std: stdDate, current: null, station_time: null, success: false };

    try {
      const proxyConfig = await this.getProxy();

      // 每个请求创建轻量级 Context，确保 Cookie 和缓存隔离
      context = await this.browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 800, height: 600 },
        proxy: proxyConfig || undefined
      });

      // 强力禁止缓存头
      await context.setExtraHTTPHeaders({
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });

      page = await context.newPage();
      
      // 优化：禁止加载图片、字体、CSS (SSR不需要CSS渲染)
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
      logger.error(`❌ [${cityConfig.name}] Realtime Error: ${error.message}`);
    } finally {
      if (page) await page.close();
      if (context) await context.close();
    }
    return result;
  }

  async syncToRedis(data) {
    if (!data.success) return;
    const { meta, date_std, current } = data;
    const key = `poly:latest:weather:${meta.station}`;

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

    await redis.hmset(key, {
        city_name: meta.name,
        target_date: date_std,
        unit: meta.unit,
        rt_current: current,
        rt_high: rtRollingHigh, 
        rt_ts: Date.now(),
        rt_station_time: dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss'),
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
    });
    
    const vizKey = `poly:viz:series:${meta.station}`;
    const ts = Date.now();
    const member = JSON.stringify({ t: current, s: 'rt', ts: ts });
    await redis.zadd(vizKey, ts, member);
    await redis.zremrangebyscore(vizKey, '-inf', ts - 86400000);

    const message = JSON.stringify({ station: meta.station, type: 'rt', ts: Date.now() });
    await redis.publish(`poly:feed:weather:${meta.station}`, message);
    logger.info(`✨ [${meta.name}] Current: ${current}°${meta.unit} | RT High: ${rtRollingHigh}°${meta.unit} | ${dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss')}`);
  }

  /**
   * 核心逻辑调整：串行执行，城市间间隔 2s
   */
  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) {
        logger.warn('🚫 No cities configured. Skipping cycle.');
        return { timestamp: Date.now() };
    }

    // 1. 确保浏览器常驻/重启
    await this.ensureBrowser();
    
    // 2. 串行异步处理，不再使用 Promise.all
    for (const city of this.cities) {
        const startTime = Date.now();
        
        const data = await this.scrapeRealtime(city);
        await this.syncToRedis(data);
        
        // 3. 间隔 2s 执行下一个城市
        // 注意：这里用 2000ms 减去已消耗的时间，或者简单硬等 2s 均可
        // 为了绝对安全，建议直接硬等 2s，确保 CPU 有充分的冷却时间
        await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    return { timestamp: Date.now() };
  }
}

export default WunderRealtimeCollector;