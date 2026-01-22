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
    // 10秒一次高频采集
    super('wunder_realtime_monitor', 10 * 1000);
    this.browser = null;
    this.cities = [];
  }

  async startBrowser() {
    if (!this.browser) {
      this.browser = await chromium.launch({ 
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      });
    }
  }

  async stopBrowser() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
  }

  async loadCitiesConfig() {
    try {
      // HGETALL 获取所有字段和值
      const rawData = await redis.hgetall('poly:config:cities');
      
      // 解析 JSON
      const loadedCities = Object.values(rawData).map(jsonStr => {
        try {
          return JSON.parse(jsonStr);
        } catch (e) {
          return null;
        }
      }).filter(item => item !== null); // 过滤掉解析失败的

      if (loadedCities.length > 0) {
        this.cities = loadedCities;
        // logger.info(`📋 Loaded ${this.cities.length} cities from Redis`);
      } else {
        logger.warn('⚠️ Redis config is empty (poly:config:cities).');
      }
    } catch (e) {
      logger.error(`❌ Failed to load cities config: ${e.message}`);
    }
  }

  /**
   * 算法对齐：F <-> C 互转
   */
  convertTemperature(val, fromUnit, toUnit) {
    if (fromUnit === toUnit) return val;
    if (fromUnit === 'F' && toUnit === 'C') {
      return Math.round((val - 32) / 1.8);
    }
    if (fromUnit === 'C' && toUnit === 'F') {
      return Math.round(val * 1.8 + 32);
    }
    return val;
  }

  /**
   * 🔥 新增：获取代理逻辑
   */
  async getProxy() {
    try {
      const proxyStr = await redis.srandmember('poly:proxylist');
      if (!proxyStr) return null;

      const parts = proxyStr.split(':');
      // 格式: IP:Port:User:Pass:Extra -> 取前4个
      if (parts.length < 4) return null;

      return {
        server: `http://${parts[0]}:${parts[1]}`,
        username: parts[2],
        password: parts[3]
      };
    } catch (e) {
      // logger.warn(`Failed to get proxy: ${e.message}`);
      return null;
    }
  }

  async scrapeRealtime(cityConfig) {
    const localNow = dayjs().tz(cityConfig.tz);
    const stdDate = localNow.format('YYYY-MM-DD'); 
    
    // 🔥 防缓存第一招：URL 随机化
    const baseUrl = `https://www.wunderground.com/weather/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}`;
    const url = `${baseUrl}?_t=${Date.now()}&rand=${Math.random().toString(36).substring(7)}`;
    
    let context = null;
    let page = null;
    const result = { meta: cityConfig, date_std: stdDate, current: null, station_time: null, success: false };

    try {
      // 🔥 新增：获取代理配置
      const proxyConfig = await this.getProxy();

      // 构建 Context 选项
      const contextOptions = {
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 800, height: 600 },
        serviceWorkers: 'block' // 🔥 防缓存第二招
      };

      // 🔥 新增：注入代理
      if (proxyConfig) {
        contextOptions.proxy = proxyConfig;
      }

      context = await this.browser.newContext(contextOptions);

      // 🔥 防缓存第三招：HTTP 头强制刷新 (穿透 CDN/代理缓存)
      await context.setExtraHTTPHeaders({
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      });

      page = await context.newPage();
      
      // 资源拦截：拦截图片、字体、媒体，极大提升速度
      await page.route('**/*.{png,jpg,jpeg,gif,svg,mp4,woff,woff2}', route => route.abort());

      // 访问实时页面
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

      // 智能等待元素出现
      await page.waitForSelector('.current-temp .wu-value', { timeout: 15000 });

      // 提取 Raw Data
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
        // 算法对齐：无论网页显示什么，强制转为配置的单位
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

    // [修正 1] 同时读取 'rt_high' 和 'target_date' 两个字段
    // 这样我们才能知道 Redis 里的那个“最高温”到底是哪天的
    const cache = await redis.hmget(key, 'rt_high', 'target_date');
    const cachedHigh = cache[0];
    const cachedDate = cache[1];

    let rtRollingHigh = current;

    // [修正 2] 增加日期校验逻辑
    // 只有当 Redis 里的日期 (cachedDate) 和当前采集的当地日期 (date_std) 完全一致时，
    // 才执行“只涨不跌”的比较逻辑。
    if (cachedHigh && cachedDate === date_std) {
        const oldHigh = parseInt(cachedHigh);
        // 逻辑：是同一天，且旧值比当前值大 -> 保持旧的最高温 (Rolling)
        if (!isNaN(oldHigh) && oldHigh > current) {
            rtRollingHigh = oldHigh;
        }
    } else {
        // 逻辑：日期不一致（说明跨天了，或者是新数据），
        // 此时直接忽略旧值，以当前的 current 作为新的一天的起始 high
        // logger.info(`📅 [${meta.name}] Day Changed: ${cachedDate} -> ${date_std}. Rolling High Reset.`);
    }

    // 3. 写入 (写入时 target_date 会更新为最新的 date_std)
    await redis.hmset(key, {
        city_name: meta.name,
        target_date: date_std, // 确保写入的是当前的时区日期
        unit: meta.unit,
        rt_current: current,
        rt_high: rtRollingHigh, 
        rt_ts: Date.now(),
        rt_station_time: dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss'),
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
    });
    
    // [新增] 写入可视化时间序列 (ZSET)
    const vizKey = `poly:viz:series:${meta.station}`;
    const ts = Date.now();
    const member = JSON.stringify({
        t: current, 
        s: 'rt',    
        ts: ts      
    });
    await redis.zadd(vizKey, ts, member);
    // 滚动清理 (保留24小时)
    await redis.zremrangebyscore(vizKey, '-inf', ts - 86400000);

    const message = JSON.stringify({
        station: meta.station,
        type: 'rt',
        ts: Date.now()
    });

    await redis.publish(`poly:feed:weather:${meta.station}`, message);
    logger.info(`✨ [${meta.name}] Current: ${current}°${meta.unit} | RT High: ${rtRollingHigh}°${meta.unit} | ${dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss')}`);
  }

  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) {
        logger.warn('🚫 No cities configured. Skipping cycle.');
        return { timestamp: Date.now() };
    }
    await this.startBrowser();
    
    // 并发执行
    const promises = this.cities.map(async (c) => {
      const d = await this.scrapeRealtime(c);
      await this.syncToRedis(d);
      return d;
    });

    const results = await Promise.all(promises);
    await this.stopBrowser();
    
    return { timestamp: Date.now() };
  }
}

export default WunderRealtimeCollector;