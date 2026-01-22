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

class WunderHistoryCollector extends BaseCollector {
  constructor() {
    super('wunder_history_monitor', 60 * 1000); 
    this.browser = null;
    this.cities = [];
  }

  async startBrowser() {
    if (!this.browser) {
      logger.info('🚀 [PolyWeather] Launching Browser...');
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

  convertTemperature(val, fromUnit, toUnit) {
    if (fromUnit === toUnit) return val;
    if (fromUnit === 'F' && toUnit === 'C') return Math.round((val - 32) / 1.8);
    if (fromUnit === 'C' && toUnit === 'F') return Math.round(val * 1.8 + 32);
    return val;
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

  async scrapeHistory(cityConfig) {
    const localNow = dayjs().tz(cityConfig.tz);
    const urlDate = `${localNow.year()}-${localNow.month() + 1}-${localNow.date()}`;
    const stdDate = localNow.format('YYYY-MM-DD'); 
    
    const url = `https://www.wunderground.com/history/daily/${cityConfig.country}/${cityConfig.city}/${cityConfig.station}/date/${urlDate}`;
    
    let context = null;
    let page = null;
    const result = { 
      meta: cityConfig,
      date_std: stdDate,
      local_time_str: localNow.format('YYYY-MM-DD HH:mm:ss'),
      high: null, 
      low: null, 
      success: false 
    };

    try {
      // 🔥 新增：获取代理配置
      const proxyConfig = await this.getProxy();

      // 构建 Context 选项
      const contextOptions = {
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      };

      // 🔥 新增：注入代理
      if (proxyConfig) {
        contextOptions.proxy = proxyConfig;
      }

      context = await this.browser.newContext(contextOptions);
      page = await context.newPage();

      // === 关键修复：开启资源拦截 ===
      // 你之前觉得慢，就是因为这一行被注释了。Wunderground 图片广告非常大，必须拦截！
      await page.route('**/*.{png,jpg,jpeg,gif,svg,mp4,woff,woff2}', route => route.abort());

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });

      // === 优化等待逻辑 ===
      // 不再使用死板的 waitForTimeout(2000)，改用智能等待，表格一出来就马上抓
      try {
        await page.waitForSelector('.observation-table tbody tr', { timeout: 15000 });
        
        const obsTable = await page.$('.observation-table');
        if (obsTable) {
          const tempCells = await obsTable.$$('tbody tr td:nth-child(2)');
          const temperatures = [];
          let detectedUnit = 'F'; // 默认 F

          for (const cell of tempCells) {
            const text = await cell.textContent();
            // 自动探测单位
            if (text.includes('C')) detectedUnit = 'C';
            
            const match = text.match(/-?\d+/);
            if (match) {
              const rawTemp = parseInt(match[0]);
              if (rawTemp > -60 && rawTemp < 140) {
                temperatures.push(rawTemp);
              }
            }
          }
          
          if (temperatures.length > 0) {
            // 使用算法统一转换，不再点击页面切换单位（这能极大提升速度和稳定性）
            const cleanTemps = temperatures.map(t => this.convertTemperature(t, detectedUnit, cityConfig.unit));

            result.high = Math.max(...cleanTemps);
            result.low = Math.min(...cleanTemps);
            result.success = true;
            // logger.info(`✅ [${cityConfig.name}] Scraped: High ${result.high}°${cityConfig.unit}`);
          }
        }
      } catch (e) {
        logger.warn(`⚠️ [${cityConfig.name}] Table load timeout: ${e.message}`);
      }

    } catch (error) {
      logger.error(`❌ [${cityConfig.name}] Scrape Error: ${error.message}`);
    } finally {
      if (page) await page.close();
      if (context) await context.close();
    }

    return result;
  }

  // === 这里严格使用了你提供的逻辑 ===
  async saveToRedis(data) {
    if (!data.success) return;
    const { meta, date_std, high, low } = data;
    const key = `poly:latest:weather:${meta.station}`;

    // History 不需要读取旧值比较，因为 History 只要抓到了就是当天的权威值
    // 直接覆盖写入 hist_ 字段
    await redis.hmset(key, {
        city_name: meta.name,
        target_date: date_std,
        hist_high: high, // 这是 History 视角的最高温
        hist_low: low,
        hist_ts: Date.now(),
        hist_station_time: dayjs().tz(meta.tz).format('YYYY-MM-DD HH:mm:ss'),
        updated_at: dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm:ss')
    });

    const message = JSON.stringify({
        station: meta.station,
        type: 'hist',
        ts: Date.now()
    });

    await redis.publish(`poly:feed:weather:${meta.station}`, message);
    logger.info(`📚 [${meta.name}] History: High ${high}°${meta.unit}`);
  }

  async fetch() {
    await this.loadCitiesConfig();
    if (this.cities.length === 0) {
        logger.warn('🚫 No cities configured. Skipping cycle.');
        return { timestamp: Date.now() };
    }

    await this.startBrowser();
    
    // 如果还是觉得卡，可以将 Promise.all 改为 for..of 串行执行
    const promises = this.cities.map(async (cityConfig) => {
      const result = await this.scrapeHistory(cityConfig);
      if (result.success) {
        await this.saveToRedis(result);
      }
      return result;
    });

    const results = await Promise.all(promises);
    await this.stopBrowser();

    return { 
      timestamp: Date.now(), 
      summary: `${results.filter(r => r.success).length}/${this.cities.length} updated` 
    };
  }
}

export default WunderHistoryCollector;